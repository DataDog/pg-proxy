package pool

import (
	"context"
	"fmt"
	"net"
)

// LookupHost resolves at least one IP for a PostgreSQL hostname.
func LookupHost(ctx context.Context, host string) ([]string, error) {
	ips, err := (&net.Resolver{}).LookupHost(ctx, host)
	if err == nil && len(ips) == 0 {
		err = fmt.Errorf("did not resolve any IPs for postgres host %s", host)
	}
	return ips, err
}
