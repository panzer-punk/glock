package lock

import (
	"testing"

	"github.com/google/uuid"
)

func TestUUIDSecretFactory(t *testing.T) {
	a := UUIDSecretFactory()
	b := UUIDSecretFactory()

	if a == "" || b == "" {
		t.Fatal("expected non-empty UUID string")
	}
	if a == b {
		t.Fatal("expected unique secrets from UUIDSecretFactory")
	}

	if _, err := uuid.Parse(a); err != nil {
		t.Fatalf("expected valid UUID, got %q: %v", a, err)
	}
	if _, err := uuid.Parse(b); err != nil {
		t.Fatalf("expected valid UUID, got %q: %v", b, err)
	}
}
