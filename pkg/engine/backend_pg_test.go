//go:build pgtest

package engine

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/railgrid/kuery/pkg/store"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// This file makes the entire engine test suite run against a real PostgreSQL
// instead of SQLite, under `go test -tags pgtest ./pkg/engine/...`. A single
// container is shared across the package; each test gets its own schema so the
// tests stay isolated (and safe to run with t.Parallel). Because every
// behavioral test goes through setupTestStore -> newBackendStore, this gives
// real-Postgres coverage of every feature — filters, projections, every
// relation type, transitive CTEs, cross-cluster, cluster-root, label/tenant
// scoping, pagination, ordering — which is where the dialect-specific SQL bugs
// (uuid/jsonb UNION typing, ambiguous columns, @> containment) actually live.

var (
	pgBaseDSN string       // URL DSN to the shared container database
	pgAdminDB *sql.DB      // admin connection used to CREATE/DROP per-test schemas
	pgSchemaN atomic.Int64 // monotonic counter for unique schema names
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithDatabase("kuery_test"),
		tcpostgres.WithUsername("kuery"),
		tcpostgres.WithPassword("kuery"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pgtest: start postgres container:", err)
		os.Exit(1)
	}

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintln(os.Stderr, "pgtest: connection string:", err)
		_ = container.Terminate(ctx)
		os.Exit(1)
	}
	pgBaseDSN = dsn

	pgAdminDB, err = sql.Open("pgx", dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pgtest: open admin db:", err)
		_ = container.Terminate(ctx)
		os.Exit(1)
	}

	code := m.Run()

	pgAdminDB.Close()
	_ = container.Terminate(ctx)
	os.Exit(code)
}

// newBackendStore creates an isolated, migrated Postgres store for one test by
// giving it a fresh schema on the shared container.
func newBackendStore(t *testing.T) store.Store {
	t.Helper()

	schema := fmt.Sprintf("t%d", pgSchemaN.Add(1))
	if _, err := pgAdminDB.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatalf("create schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		if _, err := pgAdminDB.Exec("DROP SCHEMA " + schema + " CASCADE"); err != nil {
			t.Logf("drop schema %s: %v", schema, err)
		}
	})

	s, err := store.NewStore(store.Config{
		Driver: "postgres",
		// search_path is a connect-time runtime parameter honored by pgx on
		// every pooled connection, so AutoMigrate creates the tables in (and all
		// queries resolve against) this test's private schema.
		DSN: withSearchPath(pgBaseDSN, schema),
	})
	if err != nil {
		t.Fatalf("open postgres store (schema %s): %v", schema, err)
	}
	if err := s.AutoMigrate(); err != nil {
		t.Fatalf("migrate (schema %s): %v", schema, err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func withSearchPath(dsn, schema string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}
