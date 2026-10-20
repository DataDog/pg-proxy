// Package link associates a short-lived Session with a pooled Postgres
// connection: expectation bookkeeping for the wire protocol, prepared-statement
// metadata on the conn, and acquire/release lifecycle (including optional role
// passthrough via RoleSession). Routing and pool acquire stay with the embedder;
// New wraps an already-acquired *pgxpool.Conn.
package link
