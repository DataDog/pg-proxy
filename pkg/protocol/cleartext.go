package protocol

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"

	pgproxy "github.com/DataDog/pg-proxy"
)

// cleartextMetricName counts cleartext password exchanges, tagged by outcome.
//
// TODO: make the namespace configurable. "dd.orgstore" is an internal Datadog
// name; changing it means emitting both for a window, since dashboards and
// monitors depend on it.
const cleartextMetricName = "dd.orgstore.pg_proxy.protocol.cleartext"

// GetCleartextPassword asks the client for a cleartext password and returns it.
//
// The caller is responsible for verifying the password; this only performs the
// protocol exchange. Telemetry goes to the client the embedder supplied via
// pgproxy.Bootstrap, and is dropped if it supplied none.
func GetCleartextPassword(ex Exchange) (password string, err error) {
	defer func() {
		outcome := "outcome:successful"
		if err != nil {
			outcome = "outcome:error"
		}
		count(cleartextMetricName, []string{outcome})
	}()

	// Assigns the named return rather than shadowing it, so the deferred metric
	// sees the failure.
	if err = ex.Send(&pgproto3.AuthenticationCleartextPassword{}); err != nil {
		return "", fmt.Errorf("error sending message to get password: %w", err)
	}

	encMsg, err := ex.Receive()
	if err != nil {
		return "", fmt.Errorf("error receiving password message: %w", err)
	}

	m, ok := encMsg.(*pgproto3.PasswordMessage)
	if !ok {
		// Only the type. The client is unauthenticated at this point and the
		// error reaches the logs, so the message contents must not: they would
		// put attacker-controlled text of arbitrary length there.
		return "", fmt.Errorf("expected password message request, but received: %T", encMsg)
	}

	return m.Password, nil
}

// count emits a counter.
//
// TODO: nothing about this is protocol-specific. This plus its gauge and timing
// equivalents move to an internal package once a second one needs them.
func count(name string, tags []string) {
	// Metrics are best effort: a submission failure must not fail the exchange.
	_ = pgproxy.Metrics().Count(name, 1, tags, 1)
}
