package env

import (
	"testing"
	"time"
)

func TestEffectiveHeartbeatTimeoutRoundsUpToWireSeconds(t *testing.T) {
	previousHeartbeat := Heartbeat
	previousTimeout := HeartbeatTimeout
	defer func() {
		Heartbeat = previousHeartbeat
		HeartbeatTimeout = previousTimeout
	}()

	Heartbeat = 500 * time.Millisecond
	HeartbeatTimeout = 0
	if got := EffectiveHeartbeatTimeout(); got != time.Second {
		t.Fatalf("implicit subsecond timeout = %v, want 1s", got)
	}

	HeartbeatTimeout = 500 * time.Millisecond
	if got := EffectiveHeartbeatTimeout(); got != time.Second {
		t.Fatalf("explicit subsecond timeout = %v, want 1s", got)
	}
}
