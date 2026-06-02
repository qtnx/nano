package cluster

import (
	"sync"
	"testing"
	"time"
)

func TestRemovePoolWaitsForInFlightCreate(t *testing.T) {
	const addr = "stale:8088"
	rc := newRPCClient()
	dialLock := &sync.Mutex{}
	dialLock.Lock()
	rc.dialing[addr] = dialLock

	done := make(chan struct{})
	go func() {
		rc.removePool(addr)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("removePool returned while createConnPool was still in-flight")
	case <-time.After(50 * time.Millisecond):
	}

	rc.Lock()
	rc.pools[addr] = &connPool{}
	rc.Unlock()
	dialLock.Unlock()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("removePool did not return after in-flight create completed")
	}

	rc.RLock()
	_, ok := rc.pools[addr]
	rc.RUnlock()
	if ok {
		t.Fatalf("pool for %s was republished after removePool", addr)
	}
}
