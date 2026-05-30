package env

import "testing"

// M19: closing the shutdown channel must be idempotent (no panic on a second
// close).
func TestCloseDie_Idempotent(t *testing.T) {
	ResetDie()
	defer ResetDie()

	CloseDie()
	CloseDie() // must not panic

	select {
	case <-Die:
	default:
		t.Fatal("Die should be closed after CloseDie")
	}
}

// M19: ResetDie installs a fresh, open channel and re-arms CloseDie so a new run
// can shut down again.
func TestResetDie(t *testing.T) {
	ResetDie()
	defer ResetDie()

	CloseDie()
	ResetDie()

	select {
	case <-Die:
		t.Fatal("Die should be open after ResetDie")
	default:
	}

	CloseDie()
	select {
	case <-Die:
	default:
		t.Fatal("Die should be closed after CloseDie following ResetDie")
	}
}
