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

func TestSenderPOSTSRawBodyAndHeaders(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		require.Equal(t, http.MethodPost, request.Method)
		require.Equal(t, "secret", request.Header.Get("X-Test"))
		require.Equal(t, "text/plain", request.Header.Get("Content-Type"))
		body, err := io.ReadAll(request.Body)
		require.NoError(t, err)
		require.Equal(t, "body", string(body))
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})}
	sender := NewClient(client)
	require.NoError(t, sender.SendWithOptions(context.Background(), "https://example.com/hook", "POST", map[string]string{"X-Test": "secret", "Content-Type": "text/plain"}, notification.Message{Title: "title", Body: "body"}))
}

func TestSenderGETLeavesQueryUntouched(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		require.Equal(t, http.MethodGet, request.Method)
		require.Equal(t, "test", request.URL.Query().Get("source"))
		require.Empty(t, request.URL.Query().Get("title"))
		require.Empty(t, request.URL.Query().Get("body"))
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok")), Header: make(http.Header)}, nil
	})}
	sender := NewClient(client)
	require.NoError(t, sender.SendWithOptions(context.Background(), "https://example.com/hook?source=test", "GET", nil, notification.Message{Title: "title", Body: "body"}))
}
