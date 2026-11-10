// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package scram

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHMACFunction(t *testing.T) {
	// Test with known values
	key := []byte("key")
	message := []byte("The quick brown fox jumps over the lazy dog")

	result := hmacSHA256(key, message)

	// Verify length (SHA-256 produces 32 bytes)
	assert.Equal(t, 32, len(result))

	// Verify it's deterministic
	result2 := hmacSHA256(key, message)
	assert.Equal(t, result, result2)

	// Verify different inputs produce different outputs
	result3 := hmacSHA256([]byte("different_key"), message)
	assert.NotEqual(t, result, result3)
}

func TestHashFunction(t *testing.T) {
	// Test with known input
	input := "hello world"
	result := hash(input)

	// Verify length (SHA-256 produces 32 bytes)
	assert.Equal(t, 32, len(result))

	// Verify it's deterministic
	result2 := hash(input)
	assert.Equal(t, result, result2)

	// Verify different inputs produce different outputs
	result3 := hash("different input")
	assert.NotEqual(t, result, result3)
}

func TestHiFunction(t *testing.T) {
	password := []byte("password")
	salt := []byte("salt")
	iterations := 1000

	result := hi(password, salt, iterations)

	// Verify length (should be same as HMAC output)
	assert.Equal(t, 32, len(result))

	// Verify it's deterministic
	result2 := hi(password, salt, iterations)
	assert.Equal(t, result, result2)

	// Verify different inputs produce different outputs
	result3 := hi([]byte("different"), salt, iterations)
	assert.NotEqual(t, result, result3)

	// Verify iteration count affects result
	result4 := hi(password, salt, 2000)
	assert.NotEqual(t, result, result4)
}

// Expected bytes were checked against libpq 18.6 SCRAM verifiers. In particular,
// libpq checks the mapped input before NFKC and rejects map-to-empty passwords.
var passwordVectors = []struct {
	name, password, prepared string
}{
	{"ASCII", "Pass word!123", "Pass word!123"},
	{"mixed Unicode", "abc密码", "abc密码"},
	{"Unicode only", "密码", "密码"},
	{"mapped to nothing", "I\u00adX", "IX"},
	{"non-ASCII space", "a\u00a0b", "a b"},
	{"overlapping mappings", "a\u200bb", "a b"},
	{"NFKC", "\u2168", "IX"},
	{"combining", "e\u0301", "é"},
	{"mapped to empty", "\u00ad", "\u00ad"},
	{"prohibited", "a\u00ad\a", "a\u00ad\a"},
	{"unassigned", "\u00ad\u0221", "\u00ad\u0221"},
	{"invalid bidi", "a\u00ad\u0627", "a\u00ad\u0627"},
	{"prohibited before NFKC", "\u0340", "\u0340"},
	{"unassigned before NFKC", "\u1d2c", "\u1d2c"},
	{"valid bidi", "\u0627\u0031\u0628", "\u0627\u0031\u0628"},
	{"invalid UTF-8", "a\u00ad\xff", "a\u00ad\xff"},
}

func TestNormalizeFunction(t *testing.T) {
	for _, tt := range passwordVectors {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.prepared, normalize(tt.password))
		})
	}
}

func TestGenSalt(t *testing.T) {
	// Test various sizes
	sizes := []int{8, 16, 18, 32}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("%d bytes", size), func(t *testing.T) {
			salt, err := genSalt(size)
			assert.NoError(t, err)
			assert.Equal(t, size, len(salt))

			// Generate another salt and verify they're different
			salt2, err := genSalt(size)
			assert.NoError(t, err)
			assert.NotEqual(t, salt, salt2)
		})
	}
}

func TestEncodeB64(t *testing.T) {
	input := []byte("hello world")
	result := encodeB64(input)

	// Verify it's valid base64
	decoded := make([]byte, base64.StdEncoding.DecodedLen(len(result)))
	n, err := base64.StdEncoding.Decode(decoded, result)
	assert.NoError(t, err)
	assert.Equal(t, input, decoded[:n])
}

// Benchmark tests
func BenchmarkHMACFunction(b *testing.B) {
	key := []byte("benchmark_key")
	message := []byte("benchmark message for HMAC testing")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hmacSHA256(key, message)
	}
}

func BenchmarkHiFunction(b *testing.B) {
	password := []byte("password")
	salt, _ := genSalt(16)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hi(password, salt, 4096)
	}
}
