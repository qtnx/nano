package nano

import (
	"errors"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lonng/nano/session"
)

func TestChannel_Add(t *testing.T) {
	c := NewGroup("test_add")

	var paraCount = 100
	w := make(chan bool, paraCount)
	for i := 0; i < paraCount; i++ {
		go func(id int) {
			s := session.New(nil)
			s.Bind(int64(id + 1))
			c.Add(s)
			w <- true
		}(i)
	}

	for i := 0; i < paraCount; i++ {
		<-w
	}

	if c.Count() != paraCount {
		t.Fatalf("count expect: %d, got: %d", paraCount, c.Count())
	}

	n := rand.Int63n(int64(paraCount)) + 1
	if !c.Contains(n) {
		t.Fail()
	}

	// leave
	c.LeaveAll()
	if c.Count() != 0 {
		t.Fail()
	}
}

// --- test doubles -----------------------------------------------------------

// mockEntity is a session.NetworkEntity whose Push returns a configurable error.
type mockEntity struct {
	pushErr error
}

func (m *mockEntity) Push(route string, v interface{}) error        { return m.pushErr }
func (m *mockEntity) RPC(route string, v interface{}) error         { return nil }
func (m *mockEntity) LastMid() uint64                               { return 0 }
func (m *mockEntity) Response(v interface{}) error                  { return nil }
func (m *mockEntity) ResponseMid(mid uint64, v interface{}) error   { return nil }
func (m *mockEntity) Close() error                                  { return nil }
func (m *mockEntity) RemoteAddr() net.Addr                          { return nil }
func (m *mockEntity) OriginalSid() int64                            { return 0 }

// blockingEntity blocks inside Push until release is closed, signalling entry
// (once) via entered. Used to widen the fan-out window for lock-holding tests.
type blockingEntity struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *blockingEntity) Push(route string, v interface{}) error {
	e.once.Do(func() { close(e.entered) })
	<-e.release
	return nil
}
func (e *blockingEntity) RPC(route string, v interface{}) error       { return nil }
func (e *blockingEntity) LastMid() uint64                             { return 0 }
func (e *blockingEntity) Response(v interface{}) error                { return nil }
func (e *blockingEntity) ResponseMid(mid uint64, v interface{}) error { return nil }
func (e *blockingEntity) Close() error                                { return nil }
func (e *blockingEntity) RemoteAddr() net.Addr                        { return nil }
func (e *blockingEntity) OriginalSid() int64                          { return 0 }

func newBoundSession(t *testing.T, entity session.NetworkEntity, uid int64) *session.Session {
	t.Helper()
	s := session.New(entity)
	if err := s.Bind(uid); err != nil {
		t.Fatalf("bind: %v", err)
	}
	return s
}

// H3: a genuine (non broken-pipe) push failure must be reported, not swallowed
// by returning only the last member's error.
func TestGroup_BroadcastReportsErrors(t *testing.T) {
	c := NewGroup("h3_err")
	c.Add(newBoundSession(t, &mockEntity{}, 1))                                  // succeeds
	c.Add(newBoundSession(t, &mockEntity{pushErr: errors.New("push failed")}, 2)) // fails

	// Map iteration order is random; over many runs the failing session is not
	// always last. Every call must still surface the failure.
	for i := 0; i < 64; i++ {
		if err := c.Broadcast("route", []byte("payload")); err == nil {
			t.Fatalf("iteration %d: Broadcast swallowed a failed push", i)
		}
	}
}

// H3: the fan-out must not run while holding the group lock, otherwise a
// concurrent Leave/Add/Close is blocked for the whole broadcast.
func TestGroup_BroadcastReleasesLockDuringFanout(t *testing.T) {
	c := NewGroup("h3_lock")
	be := &blockingEntity{entered: make(chan struct{}), release: make(chan struct{})}
	c.Add(newBoundSession(t, be, 1))
	c.Add(newBoundSession(t, &mockEntity{}, 2))

	bdone := make(chan struct{})
	go func() {
		_ = c.Broadcast("route", []byte("payload"))
		close(bdone)
	}()

	<-be.entered // broadcast is now blocked inside a Push

	leaveDone := make(chan struct{})
	go func() {
		_ = c.LeaveAll()
		close(leaveDone)
	}()

	select {
	case <-leaveDone:
		// good: the lock was released before pushing
	case <-time.After(3 * time.Second):
		close(be.release)
		<-bdone
		t.Fatal("group lock held across broadcast fan-out (Leave blocked)")
	}
	close(be.release)
	<-bdone
	<-leaveDone
}

// H3: Close must mutate sessions under the lock; otherwise it races readers.
func TestGroup_CloseIsSynchronized(t *testing.T) {
	c := NewGroup("h3_close")
	for i := 0; i < 256; i++ {
		c.Add(newBoundSession(t, &mockEntity{}, int64(i+1)))
	}

	start := make(chan struct{})
	done := make(chan struct{})
	go func() {
		<-start
		for i := 0; i < 50000; i++ {
			_ = c.Count()
			_ = c.Members()
		}
		close(done)
	}()

	close(start)
	_ = c.Close()
	<-done
}

// M29: an Add racing Close must never repopulate a closed group.
func TestGroup_AddDoesNotRepopulateClosedGroup(t *testing.T) {
	const goroutines = 64
	for iter := 0; iter < 200; iter++ {
		c := NewGroup("m29")
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				s := newBoundSession(t, &mockEntity{}, int64(id+1))
				<-start
				_ = c.Add(s)
			}(i)
		}
		closeDone := make(chan struct{})
		go func() {
			<-start
			_ = c.Close()
			close(closeDone)
		}()

		close(start)
		wg.Wait()
		<-closeDone

		if n := c.Count(); n != 0 {
			t.Fatalf("iteration %d: closed group retained %d sessions", iter, n)
		}
	}
}