package lock

import "github.com/google/uuid"

type SecretFactory func() Secret

type Secret interface {
	Check(secret Secret) bool
	Value() string
}

type NullSecret struct{}

func NewNullSecret() *NullSecret {
	return &NullSecret{}
}

func (s *NullSecret) Check(secret Secret) bool {
	return true
}

func (s *NullSecret) Value() string {
	return ""
}

type UUIDSecret struct {
	uuid uuid.UUID
}

func NewUUIDSecret() *UUIDSecret {
	return &UUIDSecret{uuid: uuid.New()}
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
