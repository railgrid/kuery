//go:build !pgtest

package engine

import (
	"testing"

	"github.com/railgrid/kuery/pkg/store"
)

// newBackendStore returns a fresh in-memory SQLite store. This is the default
// test backend: fast and dependency-free. Run `go test -tags pgtest ./...` to
// run the same tests against a real PostgreSQL testcontainer instead.
func newBackendStore(t *testing.T) store.Store {
	t.Helper()
	s, err := store.NewStore(store.Config{
		Driver: "sqlite",
		DSN:    ":memory:",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AutoMigrate(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
