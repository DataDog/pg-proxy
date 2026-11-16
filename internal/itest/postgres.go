// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package itest

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
)

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

// PostgresOption customizes StartPostgres.
type PostgresOption func(*postgresOptions)

type postgresOptions struct {
	image       string
	customizers []testcontainers.ContainerCustomizer
}

// WithImage starts image instead of the default. The image must run the server
// with the official image's command line (`postgres -c ...`) and accept the
// PostgresUser superuser over TCP.
func WithImage(image string) PostgresOption {
	return func(o *postgresOptions) { o.image = image }
}

// WithContainerOptions applies Testcontainers customizers, such as mounts, to
// the container request.
func WithContainerOptions(customizers ...testcontainers.ContainerCustomizer) PostgresOption {
	return func(o *postgresOptions) { o.customizers = append(o.customizers, customizers...) }
}

// StartPostgres starts a PostgreSQL container and waits until it accepts
// connections. Callers must Terminate it.
func StartPostgres(ctx context.Context, opts ...PostgresOption) (*Postgres, error) {
	options := postgresOptions{image: PostgresImage()}
	for _, opt := range opts {
		opt(&options)
	}
	image := options.image
	customizers := append([]testcontainers.ContainerCustomizer{
		postgres.WithDatabase("postgres"), postgres.WithUsername(PostgresUser), postgres.WithPassword(PostgresPassword),
		// The official entrypoint initializes with a temporary server that does not
		// listen on TCP and then restarts. Both servers log "ready", so wait for the
		// second line: matching the first lets the port check pass while the real
		// server is still starting up (SQLSTATE 57P03).
		testcontainers.WithWaitStrategy(wait.ForAll(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		).WithDeadline(2 * time.Minute)),
	}, options.customizers...)
	container, err := postgres.Run(ctx, image, customizers...)
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

// Container returns the underlying container, for example to copy files from it.
func (p *Postgres) Container() testcontainers.Container {
	return p.container
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
		Host:     net.JoinHostPort(p.Host, strconv.Itoa(int(p.Port))),
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
	database.Port = int32(p.Port)
	database.DBName = dbname
	database.SSLMode = "disable"
	database.Username = PostgresUser
	database.Password = PostgresPassword
	return database
}
