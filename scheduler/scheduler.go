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
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/metrics"
)

const (
	messageQueueBacklog = 1 << 10
	sessionCloseBacklog = 1 << 8
)

// LocalScheduler schedules task to a customized goroutine
type LocalScheduler interface {
	Schedule(Task)
}

type Task func()

type Hook func()

var (
	chDie   = make(chan struct{})
	chExit  = make(chan struct{})
	chTasks = make(chan Task, 1<<8)
	started int32
	closed  int32
)

func try(f func()) {
	defer func() {
		if err := recover(); err != nil {
			log.Println(fmt.Sprintf("Handle message panic: %+v\n%s", err, debug.Stack()))
		}
	}()
	f()
}

//func Sched() {
//	log.Debug("[Nano] Scheduler started")
//	if atomic.AddInt32(&started, 1) != 1 {
//		return
//	}
//
//	ticker := time.NewTicker(env.TimerPrecision)
//	defer func() {
//		ticker.Stop()
//		close(chExit)
//	}()
//	var wg sync.WaitGroup
//
//	for {
//		select {
//		case <-ticker.C:
//			wg.Add(1)
//			go func() {
//				defer wg.Done()
//				cron()
//			}()
//
//		case f := <-chTasks:
//			wg.Add(1)
//			go func(t Task) {
//				defer wg.Done()
//				try(t)
//			}(f)
//
//		case <-chDie:
//			log.Println("[Nano] Scheduler stopped")
//			wg.Wait()
//			return
//		}
//	}
//}

func Sched() {
	log.Debug("[Nano] Scheduler started")
	if atomic.AddInt32(&started, 1) != 1 {
		return
	}

	// Timer processing runs on its own ticker/goroutine, independent of the
	// task-dispatch select loop below. This decouples the two subsystems
	// (C6/H1): a saturated task queue can no longer delay timer ticks, and a
	// long-running timer can no longer stall task dispatch. Timer callbacks are
	// still delivered through cron()/safecall, preserving recovery semantics.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		timerLoop()
	}()

	// chExit must not close until BOTH the timer loop and this dispatch loop
	// have returned, so Close() blocks until the scheduler is fully stopped.
	defer func() {
		wg.Wait()
		close(chExit)
	}()

	for {
		select {
		case f := <-chTasks:
			try(f)

		case <-chDie:
			log.Println("[Nano] Scheduler stopped")
			return
		}
	}
}

// timerLoop drives the periodic cron() timer processing on a dedicated ticker
// and goroutine. Keeping it off the task-dispatch loop means timer ticks fire
// on schedule regardless of how busy or blocked task dispatch is. It exits when
// the scheduler is closed (chDie).
func timerLoop() {
	ticker := time.NewTicker(env.TimerPrecision)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Wrap cron in the same recover() boundary used for tasks: an
			// unrecovered panic on this goroutine would crash the process.
			try(cron)

		case <-chDie:
			return
		}
	}
}

func Close() {
	if atomic.AddInt32(&closed, 1) != 1 {
		return
	}
	close(chDie)
	<-chExit
	// Tear down the sharded dispatcher (if enabled) so its worker goroutines do
	// not outlive the scheduler. No-op in single-scheduler mode.
	stopSharded()
	log.Println("Scheduler stopped")
}

func PushTask(task Task) {
	chTasks <- task
	metrics.SchedulePendingTasks.Set(float64(len(chTasks)))
	if env.Debug {
		log.Infof("Scheduler push task channel size %d", len(chTasks))
	}
}
