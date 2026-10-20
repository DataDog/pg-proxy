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

func TestNormalizeFunction(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "ASCII only",
			input:    "password123",
			expected: "password123",
		},
		{
			name:     "Mixed case",
			input:    "PassWord",
			expected: "PassWord",
		},
		{
			name:     "With spaces",
			input:    "pass word",
			expected: "pass word",
		},
		{
			name:     "Special characters",
			input:    "p@ssw0rd!",
			expected: "p@ssw0rd!",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalize(tt.input)
			assert.Equal(t, tt.expected, result)
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
