package lock

import (
	"strings"
	"testing"
)

const expectedUUID = "123e4567-e89b-12d3-a456-426614174000"

func TestNullSecretFactory_NewSecret(t *testing.T) {
	factory := &NullSecretFactory{}
	secret := factory.NewSecret()

	if secret.Value() != "" {
		t.Fatalf("expected empty value, got %q", secret.Value())
	}
	if !secret.Check(secret) {
		t.Fatal("expected secret to match itself")
	}
	if !secret.Check(nil) {
		t.Fatal("NullSecret should accept any secret, including nil")
	}
}

func TestNullSecretFactory_FromValue(t *testing.T) {
	factory := &NullSecretFactory{}

	for _, value := range []string{"", "ignored", expectedUUID} {
		secret, err := factory.FromValue(value)
		if err != nil {
			t.Fatalf("FromValue(%q): %v", value, err)
		}
		if secret.Value() != "" {
			t.Fatalf("FromValue(%q): expected empty value, got %q", value, secret.Value())
		}
	}
}

func TestUUIDSecretFactory_NewSecret(t *testing.T) {
	factory := &UUIDSecretFactory{}

	a := factory.NewSecret()
	b := factory.NewSecret()

	if a.Value() == "" || b.Value() == "" {
		t.Fatal("expected non-empty UUID values")
	}
	if a.Value() == b.Value() {
		t.Fatal("expected NewSecret to generate unique values")
	}
	if !a.Check(a) {
		t.Fatal("expected secret to match itself")
	}
	if a.Check(b) {
		t.Fatal("expected different secrets not to match")
	}
}

func TestUUIDSecretFactory_FromValue(t *testing.T) {
	factory := &UUIDSecretFactory{}

	secret, err := factory.FromValue(expectedUUID)
	if err != nil {
		t.Fatalf("FromValue: %v", err)
	}
	if secret.Value() != expectedUUID {
		t.Fatalf("expected %q, got %q", expectedUUID, secret.Value())
	}

	other, err := factory.FromValue(expectedUUID)
	if err != nil {
		t.Fatalf("FromValue second call: %v", err)
	}
	if !secret.Check(other) {
		t.Fatal("expected secrets from same value to match")
	}
}

func TestUUIDSecretFactory_FromValue_InvalidUUID(t *testing.T) {
	factory := &UUIDSecretFactory{}

	secret, err := factory.FromValue("not-a-uuid")
	if err == nil {
		t.Fatal("expected error for invalid UUID")
	}
	if secret != nil {
		t.Fatal("expected nil secret on error")
	}
	if !strings.Contains(err.Error(), "invalid UUID") {
		t.Fatalf("expected wrapped invalid UUID error, got %v", err)
	}
}

func TestUUIDSecret_CheckNil(t *testing.T) {
	factory := &UUIDSecretFactory{}
	secret, err := factory.FromValue(expectedUUID)
	if err != nil {
		t.Fatalf("FromValue: %v", err)
	}

	if secret.Check(nil) {
		t.Fatal("expected Check(nil) to be false")
	}
}

func TestUUIDSecret_CheckDifferentValue(t *testing.T) {
	factory := &UUIDSecretFactory{}

	first, err := factory.FromValue(expectedUUID)
	if err != nil {
		t.Fatalf("FromValue first: %v", err)
	}

	second, err := factory.FromValue("00000000-0000-0000-0000-000000000001")
	if err != nil {
		t.Fatalf("FromValue second: %v", err)
	}

	if first.Check(second) {
		t.Fatal("expected different UUID secrets not to match")
	}
}

func TestNullSecret_CheckDifferentTypes(t *testing.T) {
	null, _ := (&NullSecretFactory{}).FromValue("")
	uuid, _ := (&UUIDSecretFactory{}).FromValue(expectedUUID)

	if !null.Check(uuid) {
		t.Fatal("NullSecret should accept any secret")
	}
	if !null.Check(&NullSecret{}) {
		t.Fatal("NullSecret should accept another NullSecret")
	}
}
