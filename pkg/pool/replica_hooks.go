package pool

import (
	"context"
	"math/rand"
	"net/netip"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// InstanceIDKey is the pgx connection metadata key for a resolved instance.
const InstanceIDKey = "instance_id"

// NewBeforeAcquireHook gates candidate connections on their instance health.
func NewBeforeAcquireHook(
	isHealthy func(string) bool,
	onAcquire func(string),
) func(context.Context, *pgx.Conn) bool {
	return func(_ context.Context, conn *pgx.Conn) bool {
		rawInstanceID, found := conn.PgConn().CustomData()[InstanceIDKey]
		if !found {
			return false
		}
		instanceID, ok := rawInstanceID.(string)
		if !ok {
			return false
		}
		if onAcquire != nil {
			onAcquire(instanceID)
		}
		return isHealthy(instanceID)
	}
}

// NewLookupFunc returns shuffled instance addresses for pgx connection attempts.
func NewLookupFunc(resolve func() []string) pgconn.LookupFunc {
	return func(_ context.Context, _ string) ([]string, error) {
		instances := resolve()
		rand.Shuffle(len(instances), func(i, j int) {
			instances[i], instances[j] = instances[j], instances[i]
		})
		return instances, nil
	}
}

// NewAfterConnectHook tags each pgx connection with its remote instance.
func NewAfterConnectHook(onConnect func(string)) func(context.Context, *pgx.Conn) error {
	return func(_ context.Context, conn *pgx.Conn) error {
		remoteAddress := conn.PgConn().Conn().RemoteAddr().String()
		ip := remoteAddress
		if address, err := netip.ParseAddrPort(remoteAddress); err == nil {
			ip = address.Addr().String()
		}
		conn.PgConn().CustomData()[InstanceIDKey] = ip
		if onConnect != nil {
			onConnect(remoteAddress)
		}
		return nil
	}
}
