package webhook

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/pkg/notification"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestSenderPOSTSJSONAndHeaders(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		require.Equal(t, http.MethodPost, request.Method)
		require.Equal(t, "secret", request.Header.Get("X-Test"))
		require.Equal(t, "application/json", request.Header.Get("Content-Type"))
		body, err := io.ReadAll(request.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{"title":"title","body":"body"}`, string(body))
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})}
	sender := NewClient(client)
	require.NoError(t, sender.SendWithOptions(context.Background(), "https://example.com/hook", "POST", map[string]string{"X-Test": "secret"}, notification.Message{Title: "title", Body: "body"}))
}

func TestSenderGETUsesMessageQuery(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		require.Equal(t, http.MethodGet, request.Method)
		require.Equal(t, "title", request.URL.Query().Get("title"))
		require.Equal(t, "body", request.URL.Query().Get("body"))
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok")), Header: make(http.Header)}, nil
	})}
	sender := NewClient(client)
	require.NoError(t, sender.SendWithOptions(context.Background(), "https://example.com/hook?source=test", "GET", nil, notification.Message{Title: "title", Body: "body"}))
}
