// Package protocol implements PostgreSQL wire-protocol conversations. It covers
// the cleartext password exchange; SCRAM-SHA-256 is in pkg/scram.
//
// Conversations are driven through an Exchange rather than a connection, so one
// can be tested by scripting messages instead of opening a connection. Exchange
// and its connection-backed implementation live here because mechanisms in other
// packages use them too.
package protocol
