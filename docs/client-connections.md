<!--
Unless explicitly stated otherwise all files in this repository are licensed
under the Apache-2.0 License.
This product includes software developed at Datadog (https://www.datadoghq.com/)
Copyright 2026-Present Datadog, Inc.
-->

# Client connections

Both the [standalone command](../README.md#oss-binary) and Go library support
client TLS using PostgreSQL's `SSLRequest` negotiation. Backend TLS is configured
separately through each database's `sslmode` and connection settings.

## Standalone TLS

Add these settings to the `proxy` section of your configuration:

```yaml
proxy:
  listener_address: '127.0.0.1:15432'
  tls_cert_file: /path/to/server.crt
  tls_key_file: /path/to/server.key
  tls_required: true
```

JSON uses `tlsCertFile`, `tlsKeyFile`, and `tlsRequired` inside `proxy`.

| Certificate and key | `tls_required` | Client connections |
| --- | --- | --- |
| Omitted | `false` (default) | Plaintext only; SSL requests receive `N` |
| Both provided | `false` | TLS or plaintext |
| Both provided | `true` | TLS required for normal startup |
| Omitted | `true` | Startup error |

Both files must be PEM encoded and form a matching certificate/key pair.
The private key must be unencrypted.
The certificate file may include intermediate certificates after the server
certificate. Missing, unreadable, malformed, or mismatched files fail startup
before backend pools open or the listener binds. TLS 1.2 and 1.3 are supported.
Relative paths use the process's working directory. Changes to settings or
certificate files require a restart; the command loads the pair once at startup.

For a local test, create a one-day self-signed certificate with an IP subject
alternative name matching the listener:

```sh
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout server.key -out server.crt -days 1 \
  -subj '/CN=localhost' -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1'
chmod 600 server.key
```

Point the proxy settings above at these files, restart the proxy, then connect
with certificate and hostname verification:

```sh
psql "host=127.0.0.1 port=15432 dbname=local_db user=postgres sslmode=verify-full sslrootcert=$PWD/server.crt" -c 'select 1'
```

For a CA-issued server certificate, use that CA's root bundle as `sslrootcert`.
Client `sslmode=require` requests encryption without guaranteeing hostname
verification; prefer `verify-full` when verifying the proxy's identity.

The command defaults to loopback trust using the configured backend username.
Opt-in [password/SCRAM authentication](client-authentication.md) allows non-loopback
listeners with required TLS and explicit database grants. TLS encrypts connections
and lets clients verify the proxy's identity. Client certificate authentication
(mTLS) and JWT authentication remain unsupported by the command.

### Other poolers

| Setting | PG Proxy command | PgBouncer | PgDog |
| --- | --- | --- | --- |
| Server certificate/key | `tls_cert_file` / `tls_key_file` | `client_tls_cert_file` / `client_tls_key_file` | `tls_certificate` / `tls_private_key` |
| Optional TLS | Pair with `tls_required: false` | `client_tls_sslmode=allow` or `prefer` | Pair with `tls_client_required=false` |
| Required TLS | Pair with `tls_required: true` | `client_tls_sslmode=require` | Pair with `tls_client_required=true` |
| Client certificate verification | Not configured by the command | `verify-ca` / `verify-full` plus `client_tls_ca_file` | `tls_client_ca_certificate` |

PgBouncer defaults to client TLS disabled. PgDog disables client TLS without
a certificate/key pair and defaults `tls_client_required` to false. Both keep
client-facing and backend TLS settings separate. See
[PgBouncer's TLS settings](https://www.pgbouncer.org/config.html#client_tls_sslmode)
and [PgDog's TLS settings](https://docs.pgdog.dev/configuration/pgdog.toml/general/#tls).

## Library TLS

When `listener.Config.RequireTLS` or `server.Config.RequireTLS` is true,
a non-nil `TLSConfig` is required before the listener binds. Normal startup
requires a completed TLS handshake before database resolution, authentication,
or session admission. Plaintext startup receives
`FATAL 08006: TLS is required`, then the connection closes.

Certificate callbacks (`GetCertificate` and `GetConfigForClient`) are supported.
Certificates need not be loaded when the listener is constructed.
With `RequireTLS=false`, optional TLS and plaintext startup are supported.

`CancelRequest` is allowed on a separate plaintext connection even when TLS
is required, for compatibility with PostgreSQL clients. Cancellation still
requires the session's PID and secret key. Cancellation over TLS also works.

See [listener configuration](../pkg/listener/listener.go) and
[server configuration](../pkg/server/server.go).

Certificate sourcing stays with the application. An embedder can supply and
rotate its own `tls.Config` instead of using the command's PEM file loader.
