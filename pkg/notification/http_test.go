package notification

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProviderResponses(t *testing.T) {
	for _, tc := range []struct {
		name, body       string
		status, wantCode int
		valid            bool
	}{
		{"sc3 success", `{"code":0}`, 200, 0, true},
		{"bark success", `{"code":200}`, 200, 200, true},
		{"provider failure", `{"code":400,"message":"secret"}`, 200, 200, false},
		{"missing code", `{}`, 200, 0, false},
		{"null code", `{"code":null}`, 200, 0, false},
		{"empty", "", 200, 0, false},
		{"invalid", "<html>error</html>", 200, 0, false},
		{"trailing data", `{"code":0}oops`, 200, 0, false},
		{"oversize", strings.Repeat(" ", MaxResponseBytes+1), 200, 0, false},
		{"http failure", `{"code":0}`, 429, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status); fmt.Fprint(w, tc.body) }))
			defer server.Close()
			err := PostJSON(context.Background(), HTTPClient(server.Client()), server.URL+"/secret", map[string]string{"body": "test"}, tc.wantCode)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "secret")
			}
		})
	}
}

func TestCancellationAndRedirect(t *testing.T) {
	calls := 0
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer destination.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	client := HTTPClient(server.Client())
	require.Error(t, PostJSON(context.Background(), client, server.URL, nil, 0))
	require.Zero(t, calls, "redirect must not forward the notification credential")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, PostJSON(ctx, client, server.URL+"/secret", nil, 0), context.Canceled)
	require.Equal(t, Timeout, HTTPClient(nil).Timeout)
}

func TestDefaultTransportRejectsPrivateEndpoints(t *testing.T) {
	for _, address := range []string{"127.0.0.1:80", "[::1]:80", "10.0.0.1:80", "169.254.169.254:80", "[fd00::1]:80"} {
		_, err := dialPublicEndpoint(context.Background(), "tcp", address)
		require.ErrorContains(t, err, "public address")
	}
}
