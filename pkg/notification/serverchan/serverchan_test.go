package serverchan

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/pkg/notification"
	"github.com/stretchr/testify/require"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSendUsesCurrentSC3Endpoint(t *testing.T) {
	client := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, "https://123.push.ft07.com/send/sctp123tTestKey.send", r.URL.String())
		require.Equal(t, http.MethodPost, r.Method)
		var body map[string]string
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, "任务到期", body["title"])
		require.Equal(t, "中文正文", body["desp"])
		require.Equal(t, "待办", body["tags"])
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"code":0}`)), Header: http.Header{}}, nil
	})}
	require.NoError(t, NewClient(client).Send(context.Background(), "", "sctp123tTestKey", notification.Message{Title: "任务到期", Body: "中文正文", Tags: "待办"}))
}

func TestEndpointValidation(t *testing.T) {
	got, err := Endpoint("SCT123abc")
	require.NoError(t, err)
	require.Equal(t, "https://sctapi.ftqq.com/SCT123abc.send", got)
	for _, key := range []string{"", "sctpBAD", "sctp12tkey/path", "sctp12tkey?title=other"} {
		_, err := Endpoint(key)
		require.Error(t, err)
		require.NotContains(t, err.Error(), key+"/send")
	}
}
