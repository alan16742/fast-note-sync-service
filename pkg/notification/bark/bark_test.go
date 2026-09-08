package bark

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/pkg/notification"
	"github.com/stretchr/testify/require"
)

func TestSendJSONUsesBodyKeyAndProviderCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/custom/push", r.URL.Path)
		var body map[string]string
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, "device-key", body["device_key"])
		require.Equal(t, "中文正文", body["body"])
		require.Equal(t, "vault", body["group"])
		require.Equal(t, "obsidian://open", body["url"])
		_, _ = w.Write([]byte(`{"code":200,"message":"success"}`))
	}))
	defer server.Close()
	require.NoError(t, NewClient(server.Client()).Send(context.Background(), server.URL+"/custom/", "device-key", notification.Message{Title: "title", Body: "中文正文", Group: "vault", URL: "obsidian://open"}))
}

func TestEndpointDefaultAndValidation(t *testing.T) {
	endpoint, err := Endpoint("")
	require.NoError(t, err)
	require.Equal(t, DefaultEndpoint+"/push", endpoint)
	for _, endpoint := range []string{"ftp://example.com", "https://user:password@example.com", "https://example.com?key=secret", "https://example.com/#fragment"} {
		_, err := Endpoint(endpoint)
		require.Error(t, err)
	}
}
