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
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// pushOnShardBlocking enqueues task on the shard for key, retrying on backlog so
// that submission order is preserved (a single producer retrying the same task
// before moving on cannot reorder tasks). Fails the test on any other error.
func pushOnShardBlocking(t *testing.T, key uint64, task Task) {
	t.Helper()
	for {
		err := PushTaskOnShard(key, task)
		if err == nil {
			return
		}
		if err != ErrSchedulerBacklog {
			t.Fatalf("PushTaskOnShard: unexpected error %v", err)
		}
		runtime.Gosched()
	}
}

// H1: tasks for the same key must run in submission order. The same key always
// maps to the same shard, and each shard is drained by a single goroutine, so
// FIFO per key is preserved even though distinct keys run concurrently.
func TestShardedSchedulerPreservesPerSessionOrder(t *testing.T) {
	EnableSharded(4)
	defer stopSharded()

	if !Sharded() {
		t.Fatal("expected sharded mode to be active after EnableSharded(4)")
	}

	const key = uint64(0xC0FFEE)
	const n = 2000

	got := make([]int, 0, n) // written only by the single shard worker
	var progress int64
	done := make(chan struct{})

	for i := 0; i < n; i++ {
		i := i
		pushOnShardBlocking(t, key, func() {
			got = append(got, i)
			if atomic.AddInt64(&progress, 1) == n {
				close(done)
			}
		})
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("only %d/%d tasks ran", atomic.LoadInt64(&progress), n)
	}

	// Safe to read got: close(done) happened-after the final append.
	for i, v := range got {
		if v != i {
			t.Fatalf("per-key ordering violated at index %d: got %d", i, v)
		}
	}
}

// H1: distinct keys may land on different shards and must be able to make
// progress concurrently (not serialized through one goroutine). The two tasks
// rendezvous: each must start before the other can finish, which is only
// possible if they run on different worker goroutines.
func TestShardedSchedulerRunsDifferentKeysConcurrently(t *testing.T) {
	const shards = 4
	EnableSharded(shards)
	defer stopSharded()

	// Choose two keys that map to distinct shards; otherwise the rendezvous
	// would be serialized and could not complete.
	k1 := uint64(1)
	var k2 uint64
	found := false
	for c := uint64(2); c < 1_000_00; c++ {
		if mix(k1)%shards != mix(c)%shards {
			k2, found = c, true
			break
		}
	}
	if !found {
		t.Fatal("could not find two keys mapping to distinct shards")
	}

	started1 := make(chan struct{})
	started2 := make(chan struct{})
	done := make(chan struct{})
	release := make(chan struct{}) // fallback so blocked tasks can exit on failure

	if err := PushTaskOnShard(k1, func() {
		close(started1)
		select {
		case <-started2:
		case <-release:
		}
	}); err != nil {
		t.Fatalf("push k1: %v", err)
	}
	if err := PushTaskOnShard(k2, func() {
		close(started2)
		select {
		case <-started1:
		case <-release:
		}
		close(done)
	}); err != nil {
		t.Fatalf("push k2: %v", err)
	}

	select {
	case <-done:
		// Both tasks were in-flight at once: progress was not serialized.
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("tasks on distinct keys did not make concurrent progress")
	}

	close(release)
}

// H1: the non-blocking single-channel variant must report backlog instead of
// blocking when the queue is full. With no scheduler goroutine draining chTasks
// in the test, the buffered channel fills and the next push must error.
func TestTryPushTaskReturnsBacklogWhenFull(t *testing.T) {
	var pushed int
	for {
		err := TryPushTask(func() {})
		if err == nil {
			pushed++
			if pushed > cap(chTasks)+1 {
				t.Fatal("TryPushTask never reported backlog on a full channel")
			}
			continue
		}
		if err != ErrSchedulerBacklog {
			t.Fatalf("expected ErrSchedulerBacklog, got %v", err)
		}
		break
	}
	if pushed == 0 {
		t.Fatal("expected at least one successful push before backlog")
	}
	// Drain exactly what we pushed so the shared channel is clean for peers.
	for i := 0; i < pushed; i++ {
		<-chTasks
	}
}

// H1: PushTaskOnShard must be non-blocking; when a shard's bounded queue is full
// it returns ErrSchedulerBacklog rather than blocking the producer.
func TestPushTaskOnShardBacklog(t *testing.T) {
	EnableSharded(1)
	defer stopSharded()
	const key = uint64(7)

	// Wedge the single shard worker so its bounded channel can be filled.
	release := make(chan struct{})
	running := make(chan struct{})
	if err := PushTaskOnShard(key, func() {
		close(running)
		<-release
	}); err != nil {
		t.Fatalf("priming task push failed: %v", err)
	}
	<-running // worker is now blocked inside the gate task

	var queued int
	for {
		err := PushTaskOnShard(key, func() {})
		if err == nil {
			queued++
			if queued > cap(chTasks)+1 {
				t.Fatal("shard never reported backlog on a full channel")
			}
			continue
		}
		if err != ErrSchedulerBacklog {
			t.Fatalf("expected ErrSchedulerBacklog, got %v", err)
		}
		break
	}
	if queued == 0 {
		t.Fatal("expected to queue at least one task before backlog")
	}

	close(release) // let the worker drain; stopSharded joins it
}

// H1: a panicking task must be recovered (via try) on a shard worker and must
// not kill the worker goroutine. A follow-up task on the SAME key (same shard,
// same goroutine) must still run.
func TestPanicInTaskIsRecovered(t *testing.T) {
	EnableSharded(2)
	defer stopSharded()
	const key = uint64(99)

	panicked := make(chan struct{})
	if err := PushTaskOnShard(key, func() {
		defer close(panicked)
		panic("boom in shard task")
	}); err != nil {
		t.Fatalf("push of panicking task failed: %v", err)
	}
	<-panicked // the task ran and panicked; try() must have recovered it

	ran := make(chan struct{})
	pushOnShardBlocking(t, key, func() { close(ran) })

	select {
	case <-ran:
		// Worker survived the panic and processed the next task on the shard.
	case <-time.After(3 * time.Second):
		t.Fatal("shard worker did not survive a panicking task")
	}
}

// Fix verification: DisableSharded (stopSharded) must run every accepted task —
// workers drain on teardown and a final sweep catches buffered tasks — so no
// accepted task is lost when sharding is turned off.
func TestDisableShardedRunsAllAcceptedTasks(t *testing.T) {
	EnableSharded(4)
	var ran int64
	const n = 200
	accepted := 0
	for i := 0; i < n; i++ {
		if err := PushTaskOnShard(uint64(i), func() { atomic.AddInt64(&ran, 1) }); err == nil {
			accepted++
		}
	}
	// stopSharded drains workers + sweeps buffers synchronously before returning.
	DisableSharded()
	if got := int(atomic.LoadInt64(&ran)); got != accepted {
		t.Fatalf("DisableSharded lost tasks: ran %d of %d accepted", got, accepted)
	}
	if Sharded() {
		t.Fatal("sharding should be disabled after DisableSharded")
	}
}
