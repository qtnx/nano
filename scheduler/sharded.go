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

// shardSet is an immutable snapshot of the active shard workers. It is
// published atomically by EnableSharded and read lock-free on the dispatch hot
// path, so PushTaskOnShard never has to take a mutex per task.
type shardSet struct {
	chans []chan Task // one bounded task channel per shard
}

var (
	// shards holds the active shardSet, or nil when running in the default
	// single-scheduler mode. Sharding is opt-in via EnableSharded.
	shards atomic.Pointer[shardSet]

	// shardDie signals shard workers to drain and exit. It is recreated by each
	// EnableSharded and closed by stopSharded.
	shardDie chan struct{}

	// shardWG tracks the shard worker goroutines so stopSharded can guarantee
	// none outlive teardown (no goroutine leaks on shutdown or between tests).
	shardWG sync.WaitGroup

	// shardMu serializes EnableSharded/stopSharded so concurrent setup and
	// teardown cannot publish or drain a half-built shard set.
	shardMu sync.Mutex
)

// EnableSharded switches the scheduler into opt-in sharded mode by starting n
// shard worker goroutines. Each shard owns a bounded task channel (sized like
// the legacy task backlog) drained by a single goroutine with per-task panic
// recovery. Tasks are routed to a shard by key (see PushTaskOnShard) so per-key
// (per-session) ordering is preserved while distinct keys run concurrently.
//
// n <= 0 is a no-op and leaves the default single-scheduler path active.
// EnableSharded is idempotent: once sharding is enabled, further calls are
// ignored. It is intended to be called once at startup.
func EnableSharded(n int) {
	if n <= 0 {
		return // stay single-scheduler
	}

	// Per-session ordering only holds across the legacy→shard cutover when no
	// work was enqueued before sharding was turned on. The framework enables
	// sharding in Node.Startup, before nano.Listen starts the dispatcher, so the
	// normal path is safe; warn if a caller flips it on after the fact.
	if atomic.LoadInt32(&started) != 0 {
		log.Println("[Nano] EnableSharded called after the scheduler started; enable sharding before nano.Listen to preserve per-session ordering")
	}
	shardMu.Lock()
	defer shardMu.Unlock()

	if shards.Load() != nil {
		return // already enabled
	}

	die := make(chan struct{})
	set := &shardSet{chans: make([]chan Task, n)}
	for i := 0; i < n; i++ {
		// Size each shard channel like the existing single task backlog.
		ch := make(chan Task, cap(chTasks))
		set.chans[i] = ch
		shardWG.Add(1)
		go shardWorker(ch, die)
	}

	// Publish shardDie before the shard set so any observer that sees sharding
	// active also sees the matching die channel.
	shardDie = die
	shards.Store(set)
}

// Sharded reports whether the sharded dispatcher is currently active.
func Sharded() bool {
	return shards.Load() != nil
}

// shardWorker is the per-shard actor loop. Because a given key always maps to
// the same shard, a single goroutine drains each shard channel in FIFO order,
// which is what preserves per-key (per-session) task ordering. Each task runs
// under try() so a panicking task is recovered and cannot take down the worker
// or the process. It exits when the shard set is torn down (die).
func shardWorker(ch chan Task, die chan struct{}) {
	defer shardWG.Done()
	for {
		select {
		case task := <-ch:
			try(task)
		case <-die:
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
	set := shards.Load()
	if set == nil {
		// Not sharded: behave like the legacy single path, but non-blocking.
		return tryPushTask(task)
	}
	// One shard => one goroutine => FIFO for a given key.
	idx := mix(key) % uint64(len(set.chans))
	select {
	case set.chans[idx] <- task:
		return nil
	default:
		return ErrSchedulerBacklog
	}
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
// call it directly to reset global state and prove no worker goroutine leaks.
func stopSharded() {
	shardMu.Lock()
	defer shardMu.Unlock()

	if shards.Load() == nil {
		return
	}

	// Stop routing to shards first so any in-flight PushTaskOnShard falls back
	// to the single path while the workers drain.
	shards.Store(nil)
	close(shardDie)
	shardWG.Wait()
	shardDie = nil
}

// DisableSharded returns the scheduler to single-scheduler mode, draining all
// shard workers. It is the inverse of EnableSharded — intended for clean
// shutdown and for tests that must not leave global sharding enabled.
func DisableSharded() {
	stopSharded()
}
