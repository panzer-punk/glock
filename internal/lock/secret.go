package lock

import (
	"github.com/google/uuid"
)

type SecretFactory func() string

var (
	UUIDSecretFactory SecretFactory = func() string {
		return uuid.NewString()
	}
)