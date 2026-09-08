package main

import (
	"context"
	"glock/internal/app"
	"glock/internal/lock"
	"glock/internal/service"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	DefaultUnixSocketPath = "/tmp/glock_test.sock"
	DefaultBucketsCnt     = 256
)

func main() {
	container := &app.Container{
		LockService: service.NewLockService(),
		Config: app.AppConfig{
			DefaultNamespace:     "default",
			DefaultTTL:           1 * time.Second,
			DefaultSecretFactory: lock.UUIDSecretFactory,
		},
	}
	application := app.NewApp(container)

	container.LockService.AddNamespace(container.Config.DefaultNamespace, DefaultBucketsCnt, container.Config.DefaultSecretFactory)

	backend := app.NewUnixSocketBackend(DefaultUnixSocketPath, application)

	errCh := make(chan error, 1)
	go func() {
		errCh <- backend.Start()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	shutdown := func(reason string) {
		log.Printf("shutting down: %s", reason)
		signal.Stop(sigCh)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := backend.Shutdown(ctx); err != nil {
			log.Printf("shutdown error: %v", err)
		}

		if err := <-errCh; err != nil {
			log.Printf("backend stopped: %v", err)
		}
	}

	select {
	case sig := <-sigCh:
		shutdown(sig.String())
	case err := <-errCh:
		if err != nil {
			log.Fatalf("backend stopped: %v", err)
		}
	}
}
