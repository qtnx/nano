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
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentRoutesAllowlistExactMatchAndCopiesRoutes(t *testing.T) {
	DisableConcurrentRoutes()
	t.Cleanup(DisableConcurrentRoutes)

	routes := []string{"Svc.Safe"}
	EnableConcurrentRoutes(routes, 1)
	routes[0] = "Svc.Unsafe"

	if !ConcurrentRoutesEnabled() {
		t.Fatal("expected concurrent routes to be enabled")
	}

	run := make(chan struct{})
	accepted, err := ScheduleConcurrentRoute("Svc.Safe", func() { close(run) })
	if err != nil || !accepted {
		t.Fatalf("safe route accepted=%v err=%v, want accepted with nil error", accepted, err)
	}
	waitForSignal(t, run, "allowlisted task")

	accepted, err = ScheduleConcurrentRoute("Svc", func() { t.Fatal("prefix route must not run") })
	if err != nil || accepted {
		t.Fatalf("prefix route accepted=%v err=%v, want not eligible", accepted, err)
	}

	accepted, err = ScheduleConcurrentRoute("Svc.Unsafe", func() { t.Fatal("mutated route must not run") })
	if err != nil || accepted {
		t.Fatalf("mutated route accepted=%v err=%v, want not eligible", accepted, err)
	}

	EnableConcurrentRoutes(nil, 1)
	if ConcurrentRoutesEnabled() {
		t.Fatal("nil routes should disable concurrent routes")
	}
	accepted, err = ScheduleConcurrentRoute("Svc.Safe", func() { t.Fatal("disabled route must not run") })
	if err != nil || accepted {
		t.Fatalf("disabled route accepted=%v err=%v, want not eligible", accepted, err)
	}
}

func TestConcurrentRequestsAcceptsArbitraryRoute(t *testing.T) {
	DisableConcurrentRoutes()
	t.Cleanup(DisableConcurrentRoutes)

	EnableConcurrentRequests(1)
	run := make(chan struct{})
	accepted, err := ScheduleConcurrentRoute("Anything.AtAll", func() { close(run) })
	if err != nil || !accepted {
		t.Fatalf("arbitrary route accepted=%v err=%v, want accepted with nil error", accepted, err)
	}
	waitForSignal(t, run, "all-request task")
}

func TestConcurrentRoutesZeroConcurrencyUsesDefaultWorkers(t *testing.T) {
	DisableConcurrentRoutes()
	t.Cleanup(DisableConcurrentRoutes)

	EnableConcurrentRoutes([]string{"Svc.Safe"}, 0)
	if !ConcurrentRoutesEnabled() {
		t.Fatal("zero concurrency should enable routes using the scheduler default")
	}
	run := make(chan struct{})
	accepted, err := ScheduleConcurrentRoute("Svc.Safe", func() { close(run) })
	if err != nil || !accepted {
		t.Fatalf("zero-default route accepted=%v err=%v, want accepted with nil error", accepted, err)
	}
	waitForSignal(t, run, "zero-default route task")

	EnableConcurrentRequests(0)
	run = make(chan struct{})
	accepted, err = ScheduleConcurrentRoute("Anything.Route", func() { close(run) })
	if err != nil || !accepted {
		t.Fatalf("zero-default all-request accepted=%v err=%v, want accepted with nil error", accepted, err)
	}
	waitForSignal(t, run, "zero-default all-request task")
}

func TestScheduleConcurrentRouteBacklogReturnsErrSchedulerBacklog(t *testing.T) {
	DisableConcurrentRoutes()
	t.Cleanup(DisableConcurrentRoutes)

	EnableConcurrentRoutes([]string{"Svc.Safe"}, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	accepted, err := ScheduleConcurrentRoute("Svc.Safe", func() {
		close(started)
		<-release
	})
	if err != nil || !accepted {
		t.Fatalf("wedging task accepted=%v err=%v, want accepted with nil error", accepted, err)
	}
	waitForSignal(t, started, "wedging task start")
	for i := 0; i < cap(chTasks); i++ {
		accepted, err = ScheduleConcurrentRoute("Svc.Safe", func() {})
		if err != nil || !accepted {
			close(release)
			t.Fatalf("buffer fill task %d accepted=%v err=%v, want accepted with nil error", i, accepted, err)
		}
	}

	accepted, err = ScheduleConcurrentRoute("Svc.Safe", func() { t.Fatal("backlogged task must not run") })
	if !accepted || !errors.Is(err, ErrSchedulerBacklog) {
		close(release)
		t.Fatalf("full executor accepted=%v err=%v, want accepted with ErrSchedulerBacklog", accepted, err)
	}
	close(release)
}

func TestScheduleConcurrentRouteBlockingWaitsThenRuns(t *testing.T) {
	DisableConcurrentRoutes()
	t.Cleanup(DisableConcurrentRoutes)

	EnableConcurrentRoutes([]string{"Svc.Safe"}, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	accepted, err := ScheduleConcurrentRoute("Svc.Safe", func() {
		close(started)
		<-release
	})
	if err != nil || !accepted {
		t.Fatalf("wedging task accepted=%v err=%v, want accepted with nil error", accepted, err)
	}
	waitForSignal(t, started, "wedging task start")
	for i := 0; i < cap(chTasks); i++ {
		accepted, err = ScheduleConcurrentRoute("Svc.Safe", func() {})
		if err != nil || !accepted {
			close(release)
			t.Fatalf("buffer fill task %d accepted=%v err=%v, want accepted with nil error", i, accepted, err)
		}
	}

	blockingReturned := make(chan struct{})
	blockingRan := make(chan struct{})
	go func() {
		accepted, err := ScheduleConcurrentRouteBlocking("Svc.Safe", func() { close(blockingRan) })
		if err != nil || !accepted {
			t.Errorf("blocking enqueue accepted=%v err=%v, want accepted with nil error", accepted, err)
		}
		close(blockingReturned)
	}()

	assertNoSignal(t, blockingReturned, "blocking enqueue returned before capacity was available")
	close(release)
	waitForSignal(t, blockingReturned, "blocking enqueue return")
	waitForSignal(t, blockingRan, "blocking task")
}

func TestConcurrentRoutePanicRecoveryWorkerSurvives(t *testing.T) {
	DisableConcurrentRoutes()
	t.Cleanup(DisableConcurrentRoutes)

	EnableConcurrentRoutes([]string{"Svc.Safe"}, 1)
	accepted, err := ScheduleConcurrentRouteBlocking("Svc.Safe", func() { panic("boom") })
	if err != nil || !accepted {
		t.Fatalf("panic task accepted=%v err=%v, want accepted with nil error", accepted, err)
	}

	survived := make(chan struct{})
	accepted, err = ScheduleConcurrentRouteBlocking("Svc.Safe", func() { close(survived) })
	if err != nil || !accepted {
		t.Fatalf("survival task accepted=%v err=%v, want accepted with nil error", accepted, err)
	}
	waitForSignal(t, survived, "post-panic task")
}

func TestDisableConcurrentRoutesDrainsAcceptedWorkAndDisablesEligibility(t *testing.T) {
	DisableConcurrentRoutes()
	t.Cleanup(DisableConcurrentRoutes)

	EnableConcurrentRoutes([]string{"Svc.Safe"}, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	var ran atomic.Int32
	accepted, err := ScheduleConcurrentRoute("Svc.Safe", func() {
		close(started)
		ran.Add(1)
		<-release
	})
	if err != nil || !accepted {
		t.Fatalf("wedging task accepted=%v err=%v, want accepted with nil error", accepted, err)
	}
	waitForSignal(t, started, "wedging task start")
	for i := 0; i < 3; i++ {
		accepted, err = ScheduleConcurrentRoute("Svc.Safe", func() { ran.Add(1) })
		if err != nil || !accepted {
			close(release)
			t.Fatalf("accepted task %d accepted=%v err=%v, want accepted with nil error", i, accepted, err)
		}
	}

	disabled := make(chan struct{})
	go func() {
		DisableConcurrentRoutes()
		close(disabled)
	}()
	assertNoSignal(t, disabled, "disable returned before running task completed")
	close(release)
	waitForSignal(t, disabled, "disable")

	if got := ran.Load(); got != 4 {
		t.Fatalf("ran %d accepted tasks, want 4", got)
	}
	if ConcurrentRoutesEnabled() {
		t.Fatal("expected concurrent routes disabled")
	}
	accepted, err = ScheduleConcurrentRoute("Svc.Safe", func() { t.Fatal("disabled route must not run") })
	if err != nil || accepted {
		t.Fatalf("after disable accepted=%v err=%v, want not eligible", accepted, err)
	}
}

func waitForSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func assertNoSignal(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal(message)
	case <-time.After(20 * time.Millisecond):
	}
}
