// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package scram authenticates a PostgreSQL client with SCRAM-SHA-256.
//
// SCRAM-SHA-256 is the challenge-response mechanism defined in RFC 5802. The
// client proves it knows the password without sending it, and the server proves
// the same in return, so neither a passive observer nor a dishonest peer learns
// the credential.
//
// Authenticate runs the whole conversation over a protocol.Exchange. It takes the
// password it should verify against, so where that comes from — a file, a secret
// store, a callback — is the caller's decision and no part of this package.
//
// For the protocol specification, see:
//   - RFC 5802: https://datatracker.ietf.org/doc/html/rfc5802
//   - PostgreSQL auth-scram.c: https://github.com/postgres/postgres/blob/master/src/backend/libpq/auth-scram.c
//
// The cryptography, as RFC 5802 defines it:
//
//	SaltedPassword  := Hi(Normalize(password), salt, iterations)
//	ClientKey       := HMAC(SaltedPassword, "Client Key")
//	StoredKey       := H(ClientKey)
//	AuthMessage     := client-first-message-bare + "," +
//	                   server-first-message + "," +
//	                   client-final-message-without-proof
//	ClientSignature := HMAC(StoredKey, AuthMessage)
//	ClientProof     := ClientKey XOR ClientSignature
//	ServerKey       := HMAC(SaltedPassword, "Server Key")
//	ServerSignature := HMAC(ServerKey, AuthMessage)
package scram
