package listener

import "errors"

const startupFailureMessage = "could not establish connection"

// AuthenticationError identifies a known failure for server-side diagnostics.
// Only the constants below are recognized. Client responses remain generic
// except for the fixed JWT-expiry message used by existing clients to recover.
type AuthenticationError string

const (
	ErrAuthenticationFailed  AuthenticationError = "authentication failed"
	ErrInvalidCredentials    AuthenticationError = "authentication failed: invalid credentials"
	ErrUnsupportedAuthOption AuthenticationError = "unsupported authentication option"
	ErrJWTExpired            AuthenticationError = "jwt authentication failed: token expired"
)

func (e AuthenticationError) Error() string { return string(e) }

var (
	errTLSRequired  = errors.New("TLS is required")
	errLoadShedding = errors.New("This pg-proxy instance is experiencing high load and has rejected the connection as a protection. Please retry.")
)

// startupFailure preserves causes for errors.Is/As without formatting hook or
// client-supplied data. Stages are assigned by the listener, never by a hook.
type startupFailure struct {
	stage string
	cause error
}

func (e *startupFailure) Error() string { return "connection startup failed during " + e.stage }
func (e *startupFailure) Unwrap() error { return e.cause }

func startupErrorAt(stage string, err error) error {
	return &startupFailure{stage: stage, cause: err}
}

func startupFailureDetails(err error, placement string) (stage, reason, message, diagnostic string) {
	stage, reason, message = placement, "startup_failure", startupFailureMessage
	diagnostic = message
	var failure *startupFailure
	if errors.As(err, &failure) {
		stage = failure.stage
		switch stage {
		case "startup_parameters":
			reason, diagnostic = "missing_user", "user name not provided"
		case "startup_options":
			reason, diagnostic = "invalid_startup_options", "invalid startup options"
		}
	}
	var missingSession cancelSessionNotFoundError
	var incorrectSecret cancelIncorrectSecretError
	switch {
	case errors.As(err, &missingSession):
		stage, reason, diagnostic = "cancel", "session_not_found", "failed to cancel request: "+missingSession.Error()
	case errors.As(err, &incorrectSecret):
		stage, reason, diagnostic = "cancel", "incorrect_secret", "failed to cancel request: "+incorrectSecret.Error()
	case errors.Is(err, errTLSRequired):
		stage, reason, message = "tls", "tls_required", "TLS is required"
		diagnostic = message
	case errors.Is(err, errLoadShedding):
		stage, reason, message = "load_shedding", "overloaded", errLoadShedding.Error()
		diagnostic = message
	case errors.Is(err, ErrAuthenticationFailed):
		reason, diagnostic = "authentication_failed", ErrAuthenticationFailed.Error()
	case errors.Is(err, ErrInvalidCredentials):
		reason, diagnostic = "invalid_credentials", ErrInvalidCredentials.Error()
	case errors.Is(err, ErrUnsupportedAuthOption):
		reason, diagnostic = "unsupported_auth_option", ErrUnsupportedAuthOption.Error()
	case errors.Is(err, ErrJWTExpired):
		reason, message, diagnostic = "token_expired", ErrJWTExpired.Error(), ErrJWTExpired.Error()
	}
	return stage, reason, message, diagnostic
}
