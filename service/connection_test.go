package service

import (
	"sync"
	"testing"
)

const paraCount = 500000

func TestNewConnectionService(t *testing.T) {
	service := newConnectionService()
	w := make(chan bool, paraCount)
	for i := 0; i < paraCount; i++ {
		go func() {
			service.Increment()
			service.SessionID()
			w <- true
		}()
	}

	for i := 0; i < paraCount; i++ {
		<-w
	}

	if service.Count() != paraCount {
		t.Error("wrong connection count")
	}

	if service.SessionID() != paraCount+1 {
		t.Error("wrong session id")
	}
}

// TestResetNodeIdRace exercises the global Connections reader against a
// concurrent ResetNodeId writer. Under -race this must stay clean: a runtime
// ResetNodeId must not race session-id generation (M25).
func TestResetNodeIdRace(t *testing.T) {
	const iter = 5000
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iter; i++ {
			_ = Connections.SessionID()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iter; i++ {
			ResetNodeId(uint64(i + 1))
		}
	}()
	wg.Wait()
}