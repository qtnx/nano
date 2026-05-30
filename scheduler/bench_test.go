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
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkDispatch compares dispatch throughput under concurrent producers for
// the legacy single task channel (one queue, one consumer goroutine) versus the
// opt-in sharded dispatcher (one queue + goroutine per shard). Both sub-benches
// use b.RunParallel so producer goroutines contend; the sharded variant fans
// distinct keys across shards to reduce that contention.
func BenchmarkDispatch(b *testing.B) {
	work := func() {} // trivial body: we measure dispatch overhead, not the task

	b.Run("Single", func(b *testing.B) {
		// A local consumer mimics the single-scheduler dispatch loop draining
		// the shared channel, without engaging the one-shot global Sched()/Close().
		stop := make(chan struct{})
		var done sync.WaitGroup
		done.Add(1)
		go func() {
			defer done.Done()
			for {
				select {
				case t := <-chTasks:
					t()
				case <-stop:
					for {
						select {
						case t := <-chTasks:
							t()
						default:
							return
						}
					}
				}
			}
		}()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				for TryPushTask(work) == ErrSchedulerBacklog {
					runtime.Gosched()
				}
			}
		})
		b.StopTimer()
		close(stop)
		done.Wait()
	})

	b.Run("Sharded", func(b *testing.B) {
		EnableSharded(runtime.GOMAXPROCS(0))
		defer stopSharded()

		var key uint64
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			// Each producer goroutine uses a distinct key so work spreads
			// across shards instead of contending on a single queue.
			k := atomic.AddUint64(&key, 1)
			for pb.Next() {
				for PushTaskOnShard(k, work) == ErrSchedulerBacklog {
					runtime.Gosched()
				}
			}
		})
		b.StopTimer()
	})
}
