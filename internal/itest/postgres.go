//go:build integration

package itest

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
)

// DefaultPostgresImage is the public PostgreSQL image pinned by digest.
const DefaultPostgresImage = "postgres:14-bookworm@sha256:5a05f61a534e50e679d27fb45953afec7821f2ffc2aa6c738b94515ca0891ac2"

// Superuser credentials of every container started by StartPostgres.
const (
	PostgresUser     = "postgres"
	PostgresPassword = "postgres"
)

// Postgres is a PostgreSQL container reachable from the host.
type Postgres struct {
	Host      string
	Port      uint16
	container *postgres.PostgresContainer
}

// StartPostgres starts a PostgreSQL container and waits until it accepts
// connections. Callers must Terminate it.
func StartPostgres(ctx context.Context) (*Postgres, error) {
	image := DefaultPostgresImage
	if override := os.Getenv("PG_PROXY_ITEST_POSTGRES_IMAGE"); override != "" {
		image = override
	}
	container, err := postgres.Run(ctx, image,
		postgres.WithDatabase("postgres"), postgres.WithUsername(PostgresUser), postgres.WithPassword(PostgresPassword),
		// The entrypoint restarts the server once after initdb, so wait for the second message.
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(time.Minute)),
	)
	pg := &Postgres{container: container}
	if err != nil {
		if container != nil {
			_ = pg.Terminate(context.Background())
		}
		return nil, fmt.Errorf("start postgres %s: %w", image, err)
	}
	if pg.Host, err = container.Host(ctx); err != nil {
		_ = pg.Terminate(context.Background())
		return nil, fmt.Errorf("postgres host: %w", err)
	}
	mapped, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		_ = pg.Terminate(context.Background())
		return nil, fmt.Errorf("postgres port: %w", err)
	}
	port, err := strconv.ParseUint(mapped.Port(), 10, 16)
	if err != nil {
		_ = pg.Terminate(context.Background())
		return nil, fmt.Errorf("postgres port %q: %w", mapped.Port(), err)
	}
	pg.Port = uint16(port)
	return pg, nil
}

// Terminate removes the container.
func (p *Postgres) Terminate(ctx context.Context) error {
	return p.container.Terminate(ctx)
}

// URL returns a superuser connection URL for database that bypasses the proxy.
func (p *Postgres) URL(database string) string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(PostgresUser, PostgresPassword),
		Host:     fmt.Sprintf("%s:%d", p.Host, p.Port),
		Path:     "/" + database,
		RawQuery: "sslmode=disable",
	}
	return u.String()
}

// Connect opens a superuser connection to database that bypasses the proxy.
func (p *Postgres) Connect(ctx context.Context, database string) (*pgx.Conn, error) {
	return pgx.Connect(ctx, p.URL(database))
}

// Exec runs statements as superuser on the postgres database, outside a transaction.
func (p *Postgres) Exec(ctx context.Context, statements ...string) error {
	conn, err := p.Connect(ctx, "postgres")
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	for _, statement := range statements {
		if _, err := conn.Exec(ctx, statement); err != nil {
			return fmt.Errorf("%s: %w", statement, err)
		}
	}
	return nil
}

// Database returns a pg-proxy database entry named name that connects to dbname
// on this container as the superuser.
func (p *Postgres) Database(name, dbname string) pgconfig.Database {
	var database pgconfig.Database
	database.Name = name
	database.Host = p.Host
	database.Port = p.Port
	database.Database = dbname
	database.SSLMode = "disable"
	database.Username = PostgresUser
	database.Password = PostgresPassword
	return database
}
