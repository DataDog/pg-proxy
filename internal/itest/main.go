// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

//go:build integration

package itest

import (
	"context"
	"fmt"
	"os"
	"time"
)

// Main runs a package's tests around shared fixtures. Call it from TestMain:
// setup starts fixtures such as a Postgres container and returns their teardown.
// Main builds the pg-proxy binary first so a build failure is reported once. The
// run callback calls
// m.Run() explicitly in TestMain; Main returns its exit code after teardown.
func Main(run func() int, setup func(context.Context) (teardown func(), err error)) int {
	defer removeBinary()
	if _, err := Binary(); err != nil {
		fmt.Fprintln(os.Stderr, "build pg-proxy:", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	teardown, err := setup(ctx)
	cancel()
	if teardown != nil {
		defer teardown()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration setup:", err)
		return 1
	}
	return run()
}
