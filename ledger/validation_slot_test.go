package ledger

import (
	"errors"
	"testing"
)

func TestValidationReferenceSlotPrefersCurrentSlotWhenAhead(t *testing.T) {
	t.Parallel()

	got := validationReferenceSlot(100, 125, nil)
	if got != 125 {
		t.Fatalf("expected current slot 125, got %d", got)
	}
}

func TestValidationReferenceSlotKeepsCurrentWhenEqual(t *testing.T) {
	t.Parallel()

	got := validationReferenceSlot(125, 125, nil)
	if got != 125 {
		t.Fatalf("expected shared slot 125, got %d", got)
	}
}

func TestValidationReferenceSlotFallsBackToTipOnError(t *testing.T) {
	t.Parallel()

	got := validationReferenceSlot(100, 125, errors.New("clock unavailable"))
	if got != 100 {
		t.Fatalf("expected tip slot 100 on error, got %d", got)
	}
}

func TestValidationReferenceSlotKeepsTipWhenAhead(t *testing.T) {
	t.Parallel()

	got := validationReferenceSlot(125, 100, nil)
	if got != 125 {
		t.Fatalf("expected tip slot 125, got %d", got)
	}
}
