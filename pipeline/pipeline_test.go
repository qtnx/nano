package pipeline

import (
	"sync"
	"testing"
	"time"

	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/session"
)

// TestProcessHandlerCanMutateChannel reproduces H12: Process holds the read
// lock across the handler loop, so a handler that mutates the same channel via
// PushBack/PushFront acquires the write lock while the read lock is held by the
// same goroutine, which deadlocks an `sync.RWMutex`.
func TestProcessHandlerCanMutateChannel(t *testing.T) {
	p := New()
	c := p.Inbound()

	c.PushBack(func(s *session.Session, msg *message.Message) error {
		// Mutating the channel from inside a handler must not deadlock.
		c.PushBack(func(*session.Session, *message.Message) error { return nil })
		return nil
	})

	s := session.New(nil)
	msg := &message.Message{}

	done := make(chan error, 1)
	go func() {
		done <- c.Process(s, msg)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Process returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Process deadlocked: handler calling PushBack blocked on the held read lock (H12)")
	}
}

// TestProcessConcurrentWithPushBack exercises Process against concurrent
// mutators to confirm the snapshot-based fix stays race-free under -race.
func TestProcessConcurrentWithPushBack(t *testing.T) {
	p := New()
	c := p.Outbound()
	c.PushBack(func(*session.Session, *message.Message) error { return nil })

	s := session.New(nil)
	msg := &message.Message{}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if err := c.Process(s, msg); err != nil {
					t.Errorf("Process returned error: %v", err)
					return
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			c.PushBack(func(*session.Session, *message.Message) error { return nil })
		}
	}()

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}
