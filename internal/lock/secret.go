package lock

import (
	"fmt"

	"github.com/google/uuid"
)

type SecretFactory interface {
	NewSecret() Secret
	FromValue(value string) (Secret, error)
}

type Secret interface {
	Check(secret Secret) bool
	Value() string
}

type NullSecretFactory struct{}

func (f *NullSecretFactory) NewSecret() Secret {
	return &NullSecret{}
}

func (f *NullSecretFactory) FromValue(value string) (Secret, error) {
	return &NullSecret{}, nil
}

type NullSecret struct{}

func (s *NullSecret) Check(secret Secret) bool {
	return true
}

func (s *NullSecret) Value() string {
	return ""
}

type UUIDSecretFactory struct{}

func (f *UUIDSecretFactory) NewSecret() Secret {
	return &UUIDSecret{uuid: uuid.New()}
}

func (f *UUIDSecretFactory) FromValue(value string) (Secret, error) {
	uuid, err := uuid.Parse(value)
	if err != nil {
		return nil, fmt.Errorf("invalid UUID: %w", err)
	}
	return &UUIDSecret{uuid: uuid}, nil
}

type UUIDSecret struct {
	uuid uuid.UUID
}

func (s *UUIDSecret) Check(secret Secret) bool {
	if secret == nil {
		return false
	}

	return s.Value() == secret.Value()
}

func (s *UUIDSecret) Value() string {
	return s.uuid.String()
}
