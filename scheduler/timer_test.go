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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
)

func TestNewTimer(t *testing.T) {
	var exists = struct {
		timers        int
		createdTimes  int
		closingTimers int
	}{
		timers:        len(timerManager.timers),
		createdTimes:  len(timerManager.createdTimer),
		closingTimers: len(timerManager.closingTimer),
	}

	const tc = 1000
	var counter int64
	for i := 0; i < tc; i++ {
		NewTimer(1*time.Millisecond, func() {
			atomic.AddInt64(&counter, 1)
		})
	}

	<-time.After(5 * time.Millisecond)
	cron()
	cron()
	if counter != tc*2 {
		t.Fatalf("expect: %d, got: %d", tc*2, counter)
	}

	if len(timerManager.timers) != exists.timers+tc {
		t.Fatalf("timers: %d", len(timerManager.timers))
	}

	if len(timerManager.createdTimer) != exists.createdTimes {
		t.Fatalf("createdTimer: %d", len(timerManager.createdTimer))
	}

	if len(timerManager.closingTimer) != exists.closingTimers {
		t.Fatalf("closingTimer: %d", len(timerManager.closingTimer))
	}
}

func TestNewAfterTimer(t *testing.T) {
	var exists = struct {
		timers        int
		createdTimes  int
		closingTimers int
	}{
		timers:        len(timerManager.timers),
		createdTimes:  len(timerManager.createdTimer),
		closingTimers: len(timerManager.closingTimer),
	}

	const tc = 1000
	var counter int64
	for i := 0; i < tc; i++ {
		NewAfterTimer(1*time.Millisecond, func() {
			atomic.AddInt64(&counter, 1)
		})
	}

	<-time.After(5 * time.Millisecond)
	cron()
	if counter != tc {
		t.Fatalf("expect: %d, got: %d", tc, counter)
	}

	if len(timerManager.timers) != exists.timers {
		t.Fatalf("timers: %d", len(timerManager.timers))
	}

	if len(timerManager.createdTimer) != exists.createdTimes {
		t.Fatalf("createdTimer: %d", len(timerManager.createdTimer))
	}

	if len(timerManager.closingTimer) != exists.closingTimers {
		t.Fatalf("closingTimer: %d", len(timerManager.closingTimer))
	}
}

type panicCondition struct{}

func (panicCondition) Check(now time.Time) bool { panic("boom") }

type neverCondition struct{}

func (neverCondition) Check(now time.Time) bool { return false }

// C6: a panic inside a timer condition must not escape cron(). cron() runs in
// the single scheduler goroutine (started as `go scheduler.Sched()`), so an
// unrecovered panic there would crash the whole process. The faulty timer must
// also be removed so it cannot panic again on every subsequent tick.
func TestCronRecoversConditionPanic(t *testing.T) {
	before := len(timerManager.timers)

	NewCondTimer(panicCondition{}, func() {
		t.Fatal("timer function must not run when its condition panics")
	})

	// Promotes and evaluates the condition timer. Without recovery around
	// condition.Check this panics out of cron and kills the scheduler.
	cron()

	if got := len(timerManager.timers); got != before {
		t.Fatalf("faulty condition timer not removed: before=%d after=%d", before, got)
	}
}

// M4: NewCondTimer must register the timer with its condition already set and
// must not use a MaxInt64 interval sentinel that overflows createAt+elapse.
func TestNewCondTimerNoOverflow(t *testing.T) {
	var fired int64
	tm := NewCondTimer(neverCondition{}, func() { atomic.AddInt64(&fired, 1) })
	defer func() {
		tm.Stop()
		cron()
	}()

	if tm.condition == nil {
		t.Fatal("condition must be set before the timer is registered")
	}
	// createAt+elapse must not overflow into the past; otherwise a timer whose
	// condition is briefly observed as nil would fire immediately.
	if tm.createAt+tm.elapse < tm.createAt {
		t.Fatalf("createAt+elapse overflows: createAt=%d elapse=%d", tm.createAt, tm.elapse)
	}

	// A false condition must never fire the timer function.
	cron()
	cron()
	if n := atomic.LoadInt64(&fired); n != 0 {
		t.Fatalf("condition timer fired %d times despite a false condition", n)
	}
}

// fakeLogger captures formatted log output so we can assert PushTask formats
// its debug message instead of leaking a literal %d verb.
type fakeLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (f *fakeLogger) add(s string) {
	f.mu.Lock()
	f.msgs = append(f.msgs, s)
	f.mu.Unlock()
}

func (f *fakeLogger) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.msgs))
	copy(out, f.msgs)
	return out
}

func (f *fakeLogger) Debug(v ...interface{})                 { f.add(fmt.Sprint(v...)) }
func (f *fakeLogger) Println(v ...interface{})               { f.add(fmt.Sprint(v...)) }
func (f *fakeLogger) Infof(format string, v ...interface{})  { f.add(fmt.Sprintf(format, v...)) }
func (f *fakeLogger) Error(v ...interface{})                 { f.add(fmt.Sprint(v...)) }
func (f *fakeLogger) Errorf(format string, v ...interface{}) { f.add(fmt.Sprintf(format, v...)) }
func (f *fakeLogger) Fatal(v ...interface{})                 { f.add(fmt.Sprint(v...)) }
func (f *fakeLogger) Fatalf(format string, v ...interface{}) { f.add(fmt.Sprintf(format, v...)) }
func (f *fakeLogger) Warn(v ...interface{})                  { f.add(fmt.Sprint(v...)) }

// L2: PushTask's debug log must use a format-style logger so the size verb is
// rendered, not printed literally as "%d".
func TestPushTaskLogFormatting(t *testing.T) {
	// Save and restore the package-level logger funcs mutated by SetLogger.
	savedDebug, savedDebugf := log.Debug, log.Debugf
	savedPrintln, savedInfof := log.Println, log.Infof
	savedFatal, savedFatalf := log.Fatal, log.Fatalf
	savedError, savedErrorf := log.Error, log.Errorf
	savedWarn := log.Warn
	defer func() {
		log.Debug, log.Debugf = savedDebug, savedDebugf
		log.Println, log.Infof = savedPrintln, savedInfof
		log.Fatal, log.Fatalf = savedFatal, savedFatalf
		log.Error, log.Errorf = savedError, savedErrorf
		log.Warn = savedWarn
	}()

	fake := &fakeLogger{}
	log.SetLogger(fake)

	savedDebugEnv := env.Debug
	env.Debug = true
	defer func() { env.Debug = savedDebugEnv }()

	PushTask(func() {})
	<-chTasks // keep the shared task channel clean for other tests

	var found bool
	for _, m := range fake.snapshot() {
		if strings.Contains(m, "Scheduler push task channel size") {
			found = true
			if strings.Contains(m, "%d") {
				t.Fatalf("debug log not formatted (literal verb leaked): %q", m)
			}
			if !strings.Contains(m, "size 1") {
				t.Fatalf("debug log did not render the queue size: %q", m)
			}
		}
	}
	if !found {
		t.Fatal("expected PushTask to emit a debug log line")
	}
}

// M4: Stop() must not write the scheduler-owned counter; doing so races with
// cron(). Run under -race.
func TestTimerStopRace(t *testing.T) {
	const n = 200
	timers := make([]*Timer, n)
	for i := range timers {
		timers[i] = NewTimer(time.Hour, func() {})
	}
	cron() // promote the new timers into the scheduler map

	var wg sync.WaitGroup
	wg.Add(2)
	start := make(chan struct{})
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 2000; i++ {
			cron()
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for _, tm := range timers {
			tm.Stop()
		}
	}()
	close(start)
	wg.Wait()
}

// M4: cron() must hold muCreatedTimer before reading createdTimer; the
// unsynchronized length check races with concurrent NewTimer appends.
// Run under -race.
func TestTimerCreatedTimerRace(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)
	start := make(chan struct{})
	created := make([]*Timer, 0, 1000)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 3000; i++ {
			cron()
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1000; i++ {
			created = append(created, NewTimer(time.Hour, func() {}))
		}
	}()
	close(start)
	wg.Wait()

	for _, tm := range created {
		tm.Stop()
	}
	cron()
	cron()
}
