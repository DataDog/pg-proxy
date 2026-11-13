// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"context"
	"errors"
	"maps"
	"net"
	"slices"
)

// PatroniProbeResult describes a completed HealthProber.Probe call, after any retries.
// A response can report either healthy or unhealthy; it does not imply HTTP 200.
type PatroniProbeResult string

const (
	PatroniProbeResponse PatroniProbeResult = "response"
	PatroniProbeTimeout  PatroniProbeResult = "timeout"
	PatroniProbeError    PatroniProbeResult = "error"
	PatroniProbeUnknown  PatroniProbeResult = "unknown"
)

func classifyPatroniProbeResult(err error) PatroniProbeResult {
	if err == nil {
		return PatroniProbeResponse
	}
	var networkError net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &networkError) && networkError.Timeout() {
		return PatroniProbeTimeout
	}
	return PatroniProbeError
}

func (s *PoolHealth) patroniComparisonSnapshot() (map[string]bool, map[string]PatroniProbeResult) {
	s.patroniObservationMu.RLock()
	defer s.patroniObservationMu.RUnlock()
	return s.state.Snapshot(), maps.Clone(s.patroniProbeResults)
}

// PatroniProbeResult summarizes final Patroni results for the IPs implicated by MismatchReason.
// Primary disagreements use only role-disagreeing IPs; membership differences use missing IPs;
// SQL role conflicts use the conflicting IPs. Matches summarize all compared IPs. Across those
// IPs, timeout takes precedence over error, then unknown, then response. Empty/missing results
// are unknown. This describes the observation, not a proven cause of a mismatch.
func (s SQLPatroniHealthSnapshot) PatroniProbeResult() PatroniProbeResult {
	ips := s.patroniProbeComparisonIPs()
	if len(ips) == 0 {
		return PatroniProbeUnknown
	}
	result := PatroniProbeResponse
	for ip := range ips {
		switch s.PatroniProbeResults[ip] {
		case PatroniProbeTimeout:
			return PatroniProbeTimeout
		case PatroniProbeError:
			result = PatroniProbeError
		case PatroniProbeResponse:
		default:
			if result != PatroniProbeError {
				result = PatroniProbeUnknown
			}
		}
	}
	return result
}

func (s SQLPatroniHealthSnapshot) patroniProbeComparisonIPs() map[string]struct{} {
	ips := make(map[string]struct{})
	switch s.MismatchReason() {
	case SQLPatroniSQLRoleConflict:
		for _, ip := range s.SQLConflictingIPs {
			ips[ip] = struct{}{}
		}
	case SQLPatroniPrimaryDisagreement:
		for ip, sqlHealthy := range s.SQLHealth {
			if sqlHealthy != s.PatroniHealth[ip] {
				ips[ip] = struct{}{}
			}
		}
		for ip, healthy := range s.PatroniHealth {
			if _, known := s.SQLHealth[ip]; !known && healthy && !slices.Contains(s.SQLRetainedIPs, ip) {
				ips[ip] = struct{}{}
			}
		}
	case SQLPatroniMembershipDifference:
		for ip := range s.SQLHealth {
			if _, known := s.PatroniHealth[ip]; !known {
				ips[ip] = struct{}{}
			}
		}
		for ip := range s.PatroniHealth {
			if _, known := s.SQLHealth[ip]; !known && !slices.Contains(s.SQLRetainedIPs, ip) {
				ips[ip] = struct{}{}
			}
		}
	default:
		for _, ip := range s.SQLRetainedIPs {
			ips[ip] = struct{}{}
		}
		for ip := range s.SQLHealth {
			ips[ip] = struct{}{}
		}
		for ip := range s.PatroniHealth {
			ips[ip] = struct{}{}
		}
	}
	return ips
}
