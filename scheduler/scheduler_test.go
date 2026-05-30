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
	"testing"
	"time"
)

// C6/H1: timer processing must run on its own goroutine/ticker, independent of
// the task-dispatch loop. A single task that blocks the dispatch goroutine must
// NOT be able to starve the periodic cron() timer tick. With the legacy combined
// select loop, a blocking task wedges the only goroutine and timers never fire;
// after the split they fire regardless.
func TestTimerStillFiresIndependentOfBusyDispatch(t *testing.T) {
	go Sched()
	defer Close()

	// Occupy the task-dispatch goroutine with a task that blocks until released.
	// In the legacy design this is the same goroutine that services the timer
	// ticker, so cron() would be starved for the whole duration.
	gate := make(chan struct{})
	PushTask(func() { <-gate })

	// A one-shot timer; if the timer subsystem is decoupled from dispatch it
	// fires on the next cron tick even though the dispatch goroutine is wedged.
	fired := make(chan struct{})
	NewAfterTimer(5*time.Millisecond, func() { close(fired) })

	select {
	case <-fired:
		// Timer fired while dispatch was blocked: the subsystems are decoupled.
	case <-time.After(3 * time.Second):
		close(gate) // release so the deferred Close() can return
		t.Fatal("timer did not fire while the task-dispatch goroutine was busy")
	}

	close(gate) // release the blocked task so Close() can drain and stop
}
