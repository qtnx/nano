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

func Sched() {
	log.Debug("[Nano] Scheduler started")
	if atomic.AddInt32(&started, 1) != 1 {
		return
	}

	ticker := time.NewTicker(env.TimerPrecision)
	defer func() {
		ticker.Stop()
		close(chExit)
	}()
	var wg sync.WaitGroup

	for {
		select {
		case <-ticker.C:
			wg.Add(1)
			go func() {
				defer wg.Done()
				cron()
			}()

		case f := <-chTasks:
			wg.Add(1)
			go func(t Task) {
				defer wg.Done()
				try(t)
			}(f)

		case <-chDie:
			log.Println("[Nano] Scheduler stopped")
			wg.Wait()
			return
		}
	}
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
//
//	for {
//		select {
//		case <-ticker.C:
//			cron()
//
//		case f := <-chTasks:
//			try(f)
//
//		case <-chDie:
//			log.Println("[Nano] Scheduler stopped")
//			return
//		}
//	}
//}

func Close() {
	if atomic.AddInt32(&closed, 1) != 1 {
		return
	}
	close(chDie)
	<-chExit
	log.Println("Scheduler stopped")
}

func PushTask(task Task) {
	chTasks <- task
	metrics.SchedulePendingTasks.Set(float64(len(chTasks)))
	if env.Debug {
		log.Println("Scheduler push task channel size %d", len(chTasks))
	}
}
