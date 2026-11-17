// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

// Package setdetect is an experiment, not production code. It compares ways
// to decide from SQL text whether a checkout may have changed session state
// that the release reset (RESET SESSION AUTHORIZATION; RESET ROLE; RESET ALL)
// restores, for a reset_mode: dynamic that skips the reset on clean checkouts.
//
// Every approach implements Detector. The labeled corpus lives in corpus/,
// its generator in gen/, and the raw results in results/. See README.md for
// how to regenerate each piece.
package setdetect
