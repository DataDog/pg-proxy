// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

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
