package pool

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestProbePatroni(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		assert.Equal(t, http.MethodGet, request.Method)
		assert.Equal(t, "https://10.0.0.1:8009/replica?lag=16mb", request.URL.String())
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"state":"running","database_system_identifier":"7688825698061787319"}`)),
		}, nil
	})}

	healthy, systemID, status, err := ProbePatroni(PatroniProbe{
		IP:     "10.0.0.1",
		Path:   "/replica?lag=16mb",
		Client: client,
	})

	require.NoError(t, err)
	assert.True(t, healthy)
	assert.Equal(t, "7688825698061787319", systemID)
	assert.Equal(t, http.StatusOK, status)
}
