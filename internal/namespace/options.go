package namespace

import "glock/internal/lock"

type Options struct {
	Buckets       uint32
	SecretFactory lock.SecretFactory
}

func DefaultOptions() *Options {
	return &Options{
		Buckets: 256,
		SecretFactory: func() lock.Secret {
			return lock.NewNullSecret()
		},
	}
}
