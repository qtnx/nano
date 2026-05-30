// Copyright (c) nano Authors. All Rights Reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package scheduler

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/metrics"
)

// ErrSchedulerBacklog is returned by the non-blocking enqueue paths
// (TryPushTask and PushTaskOnShard) when the target task queue is already full.
// Callers MUST handle it explicitly (e.g. shed the request or apply
// backpressure) rather than relying on the blocking PushTask, which can stall
// the producer when the scheduler is saturated. The dispatcher never silently
// drops a task: a full queue is always surfaced as this error.
var ErrSchedulerBacklog = errors.New("nano/scheduler: task queue backlog full")

// shardSet holds the active shard worker channels and their shared teardown
// signal. It is only ever read/replaced under shardMu.
type shardSet struct {
	chans []chan Task   // one bounded task channel per shard
	die   chan struct{} // closed by stopSharded after it has exclusive access
}

var (
	// shardMu guards shardCur. Enqueue paths take it for READ (concurrent, so
	// dispatch still scales across cores); EnableSharded/stopSharded take it for
	// WRITE. Because a writer waits for all readers, teardown is guaranteed that
	// no enqueue is mid-flight when it unpublishes the set — there is no window
	// where a task is accepted onto a shard that is being drained/abandoned.
	shardMu  sync.RWMutex
	shardCur *shardSet // active set, or nil in single-scheduler mode (guarded by shardMu)

	// shardOn is a lock-free hint for the hot pre-check Sharded(); the authoritative
	// state is shardCur under shardMu.
	shardOn atomic.Bool

	// shardWG tracks shard worker goroutines so stopSharded can guarantee none
	// outlive teardown (no goroutine leaks on shutdown or between tests).
	shardWG sync.WaitGroup
)

// EnableSharded switches the scheduler into opt-in sharded mode by starting n
// shard worker goroutines. Each shard owns a bounded task channel (sized like
// the legacy task backlog) drained by a single goroutine with per-task panic
// recovery. Tasks are routed to a shard by key (see PushTaskOnShard) so per-key
// (per-session) ordering is preserved while distinct keys run concurrently.
//
// n <= 0 is a no-op and leaves the default single-scheduler path active.
// EnableSharded is idempotent: once sharding is enabled, further calls are
// ignored. It MUST be called once at startup (the framework does so in
// Node.Startup, before nano.Listen starts the dispatcher); enabling it after
// work has been queued cannot guarantee per-session ordering across the
// legacy->shard cutover.
func EnableSharded(n int) {
	if n <= 0 {
		return // stay single-scheduler
	}

	// Per-session ordering only holds across the cutover when no work was
	// enqueued before sharding was turned on. Warn on misuse rather than
	// silently degrading the ordering guarantee.
	if atomic.LoadInt32(&started) != 0 {
		log.Println("[Nano] EnableSharded called after the scheduler started; enable sharding before nano.Listen to preserve per-session ordering")
	}

	shardMu.Lock()
	defer shardMu.Unlock()

	if shardCur != nil {
		return // already enabled
	}

	die := make(chan struct{})
	set := &shardSet{chans: make([]chan Task, n), die: die}
	for i := 0; i < n; i++ {
		// Size each shard channel like the existing single task backlog.
		ch := make(chan Task, cap(chTasks))
		set.chans[i] = ch
		shardWG.Add(1)
		go shardWorker(ch, die)
	}

	shardCur = set
	shardOn.Store(true)
}

// Sharded reports whether the sharded dispatcher is currently active. It is a
// lock-free hint; the enqueue paths re-check authoritatively under shardMu and
// fall back to the single scheduler if sharding was turned off in between.
func Sharded() bool {
	return shardOn.Load()
}

// drainChan runs every task currently buffered in ch (non-blocking) and returns
// once the channel is empty.
func drainChan(ch chan Task) {
	for {
		select {
		case task := <-ch:
			try(task)
		default:
			return
		}
	}
}

// shardWorker is the per-shard actor loop. Because a given key always maps to
// the same shard, a single goroutine drains each shard channel in FIFO order,
// which is what preserves per-key (per-session) task ordering. Each task runs
// under try() so a panicking task is recovered. On teardown (die) it drains any
// already-queued tasks before exiting so accepted work is not dropped.
func shardWorker(ch chan Task, die chan struct{}) {
	defer shardWG.Done()
	for {
		select {
		case task := <-ch:
			try(task)
		case <-die:
			drainChan(ch)
			return
		}
	}
}

// PushTaskOnShard enqueues task on the shard selected by key. The same key
// always maps to the same shard, guaranteeing per-key ordering; distinct keys
// may land on different shards and run concurrently.
//
// When sharding is not enabled it falls back to the legacy single task channel.
// Either way the enqueue is non-blocking: if the target queue is full it returns
// ErrSchedulerBacklog instead of blocking the caller (and never drops the task).
func PushTaskOnShard(key uint64, task Task) error {
	shardMu.RLock()
	set := shardCur
	if set == nil {
		shardMu.RUnlock()
		return tryPushTask(task) // not sharded: single path, non-blocking
	}
	// Holding the read lock guarantees the shard worker is alive for the
	// duration of the send, so an accepted task always has a live drainer.
	idx := mix(key) % uint64(len(set.chans))
	select {
	case set.chans[idx] <- task:
		shardMu.RUnlock()
		return nil
	default:
		shardMu.RUnlock()
		return ErrSchedulerBacklog
	}
}

// PushTaskOnShardBlocking enqueues task on the SAME shard selected for key,
// blocking until that shard has capacity. Unlike falling back to the single
// scheduler, it preserves the shard the key is confined to, so per-key
// (per-session) FIFO ordering still holds. It is the correct backpressure path
// for response-expecting work that must not be dropped under overload. When
// sharding is not enabled it blocks on the single scheduler.
//
// The read lock is held across the (possibly blocking) send: teardown takes the
// write lock and therefore waits until the send completes, which is safe because
// the shard worker keeps draining until teardown actually begins — so the send
// is guaranteed to make progress and never targets a dead shard.
func PushTaskOnShardBlocking(key uint64, task Task) {
	shardMu.RLock()
	set := shardCur
	if set == nil {
		shardMu.RUnlock()
		PushTask(task)
		return
	}
	idx := mix(key) % uint64(len(set.chans))
	set.chans[idx] <- task
	shardMu.RUnlock()
}

// TryPushTask is the non-blocking counterpart of PushTask: it enqueues task on
// the single scheduler channel and returns ErrSchedulerBacklog if the channel is
// full, rather than blocking until space becomes available.
func TryPushTask(task Task) error {
	return tryPushTask(task)
}

// tryPushTask performs a non-blocking enqueue onto the legacy single task
// channel, mirroring PushTask's pending-task metric update on success.
func tryPushTask(task Task) error {
	select {
	case chTasks <- task:
		metrics.SchedulePendingTasks.Set(float64(len(chTasks)))
		return nil
	default:
		return ErrSchedulerBacklog
	}
}

// mix scrambles a shard key with the splitmix64 finalizer so that sequential
// ids (e.g. monotonically increasing session ids) avalanche across shards
// instead of clustering on a few shards. It is branch-free and costs only a
// handful of multiplies and shifts.
func mix(key uint64) uint64 {
	key ^= key >> 30
	key *= 0xbf58476d1ce4e5b9
	key ^= key >> 27
	key *= 0x94d049bb133111eb
	key ^= key >> 31
	return key
}

// stopSharded returns the scheduler to single-scheduler mode and drains every
// shard worker goroutine. Production code reaches this through Close(); tests
// call it (via DisableSharded) to reset global state and prove no worker
// goroutine leaks.
//
// Correctness: it acquires the write lock, so it cannot proceed until every
// in-flight enqueue (which holds the read lock) has finished. Once it unpublishes
// the set under the lock, no later enqueue can reference it (they re-read
// shardCur and see nil). Only then does it close die and wait; workers drain
// their queues on die, and a final sweep covers anything left, so no accepted
// task is lost and none runs without a live worker.
func stopSharded() {
	shardMu.Lock()
	set := shardCur
	if set == nil {
		shardMu.Unlock()
		return
	}
	shardCur = nil
	shardOn.Store(false)
	shardMu.Unlock()

	// Past this point no enqueue can target set.chans (new enqueues see a nil
	// shardCur and use the single path), so closing/draining is race-free.
	close(set.die)
	shardWG.Wait()
	for _, ch := range set.chans {
		drainChan(ch)
	}
}

// DisableSharded returns the scheduler to single-scheduler mode, draining all
// shard workers. It is the inverse of EnableSharded — intended for clean
// shutdown and for tests that must not leave global sharding enabled.
func DisableSharded() {
	stopSharded()
}
