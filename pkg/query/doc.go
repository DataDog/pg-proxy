// Package query decides what to do with query-execution messages once a
// connection is authenticated: what a message from Postgres means for a
// session (DecidePgMessageAction), how to size messages for buffering, and
// how to trace that message flow (TraceState).
package query
