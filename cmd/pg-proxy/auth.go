// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package main

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"

	"github.com/DataDog/pg-proxy/pkg/auth"
	pgconfig "github.com/DataDog/pg-proxy/pkg/config"
	"github.com/DataDog/pg-proxy/pkg/listener"
	"github.com/DataDog/pg-proxy/pkg/protocol"
	"github.com/DataDog/pg-proxy/pkg/scram"
	"github.com/DataDog/pg-proxy/pkg/server"
)

type authenticationHook = func(context.Context, listener.StartupRequest, listener.StartupResult[server.ResolvedStartup], protocol.Exchange) (listener.AuthenticationResult, error)

func passwordAuthentication(shared pgconfig.Config) (authenticationHook, error) {
	users := make([]auth.StaticUser, 0, len(shared.Users))
	var issues []error
	seen := make(map[string]bool)
	for i, user := range shared.Users {
		var userIssues []error
		if user.Email != "" || user.Group != "" || len(user.PGRoles) != 0 {
			userIssues = append(userIssues, errors.New("password authentication supports only static_credentials, databases, and max_sessions in each user"))
		}
		credentials := user.StaticCredentials
		validCredentials := credentials.Username != "" && len(credentials.Username) <= 256 && !strings.ContainsRune(credentials.Username, 0) && (credentials.Password == "") != (credentials.SCRAMSHA256 == "")
		if !validCredentials {
			userIssues = append(userIssues, errors.New("each proxy user requires a username and exactly one password or SCRAM verifier"))
		}
		if seen[credentials.Username] && credentials.Username != "" {
			userIssues = append(userIssues, errors.New("proxy usernames must be unique"))
		}
		seen[credentials.Username] = true
		if len(user.Databases) == 0 {
			userIssues = append(userIssues, errors.New("each proxy user requires allowed databases"))
		}
		for j, database := range user.Databases {
			if _, exists := shared.LogicalDatabases[database]; !exists {
				userIssues = append(userIssues, fmt.Errorf("databases[%d]: allowed databases must refer to configured logical databases", j))
			}
			if database == "" {
				userIssues = append(userIssues, errors.New("allowed database names must not be empty"))
			}
		}
		var verifier *scram.Verifier
		if validCredentials {
			var err error
			if credentials.Password != "" {
				verifier, err = scram.NewVerifier(string(credentials.Password))
			} else {
				verifier, err = scram.ParseVerifier(string(credentials.SCRAMSHA256))
			}
			userIssues = append(userIssues, err)
		}
		if err := errors.Join(userIssues...); err != nil {
			issues = append(issues, configurationContext(fmt.Sprintf("users[%d]", i), err))
			continue
		}
		users = append(users, auth.StaticUser{Username: credentials.Username, Verifier: verifier, Databases: user.Databases})
	}
	if err := errors.Join(issues...); err != nil {
		return nil, err
	}
	static, err := auth.NewStatic(users)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, request listener.StartupRequest, resolved listener.StartupResult[server.ResolvedStartup], ex protocol.Exchange) (listener.AuthenticationResult, error) {
		if request.Options.PassthroughUser {
			return listener.AuthenticationResult{}, listener.ErrUnsupportedAuthOption
		}
		if err := static.Authenticate(ctx, request.User, resolved.LogicalDatabase, ex); err != nil {
			return listener.AuthenticationResult{}, err
		}
		return listener.AuthenticationResult{Service: request.User}, nil
	}, nil
}

// Only a successfully constructed authentication method may enable remote
// listeners. TLS by itself never enables remote trust.
func (c config) validateListener(authenticated bool) error {
	var issues []error
	issues = append(issues, c.validateListenerAddress(authenticated))
	if authenticated {
		if !c.requireTLS || c.tlsConfig == nil {
			issues = append(issues, errors.New("client authentication requires proxy.tls_required and a TLS certificate/key pair"))
		}
	} else if c.requireTLS && c.tlsConfig == nil {
		issues = append(issues, errors.New("proxy.tls_required requires tls_cert_file and tls_key_file"))
	}
	return errors.Join(issues...)
}

func (c config) validateListenerAddress(authenticated bool) error {
	address, err := netip.ParseAddrPort(c.listenAddress)
	if err != nil {
		if !authenticated {
			return errors.New("proxy.listener_address must use a loopback IP and port in trust mode")
		}
		return errors.New("proxy.listener_address must be an IP address and port")
	}
	if !authenticated && !address.Addr().IsLoopback() {
		return errors.New("proxy.listener_address must use a loopback IP unless client authentication is enabled")
	}
	return nil
}
