package nano

import (
	"testing"

	"github.com/lonng/nano/internal/env"
)

// M19: Shutdown must be idempotent — a second call must not panic on an
// already-closed channel.
func TestShutdown_Idempotent(t *testing.T) {
	env.ResetDie()
	defer env.ResetDie()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("second Shutdown panicked: %v", r)
		}
	}()

	Shutdown()
	Shutdown() // must not panic
}

// M19: ResetDie reopens the shutdown channel after a Shutdown so a subsequent
// Listen run does not immediately tear down on a closed channel.
func TestResetDie_Reopens(t *testing.T) {
	env.ResetDie()
	defer env.ResetDie()

	Shutdown()
	env.ResetDie()
	select {
	case <-env.Die:
		t.Fatal("Die should be open after ResetDie")
	default:
	}
}

// M33: a nil Option must be skipped during application, not invoked (which would
// panic at startup before validation).
func TestApplyOptions_SkipsNilOption(t *testing.T) {
	opt := applyOptions([]Option{nil, WithMaster(), nil})
	if !opt.IsMaster {
		t.Fatal("non-nil option was not applied")
	}
	if opt.Components == nil {
		t.Fatal("default Components not set")
	}
}
