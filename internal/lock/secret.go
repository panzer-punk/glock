package lock

import (
	"github.com/google/uuid"
)

type SecretFactory func() string

var (
	UUIDSecretFactory SecretFactory = func() string {
		// TODO return uuid.New() 16 raw bytes; NewString hex-encodes (36 chars) and Handle copies to []byte again.
		return uuid.NewString()
	}
)
