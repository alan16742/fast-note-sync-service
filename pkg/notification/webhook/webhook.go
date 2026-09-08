package webhook

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/haierkeys/fast-note-sync-service/pkg/notification"
)

// Sender delivers the common notification message to a user-configured HTTP
// endpoint. The service validates the URL and header policy before saving it.
type Sender struct{ client *http.Client }

func NewClient(client *http.Client) *Sender {
	return &Sender{client: notification.HTTPClient(client)}
}

func (s *Sender) Send(ctx context.Context, endpoint, _ string, message notification.Message) error {
	return s.SendWithOptions(ctx, endpoint, http.MethodPost, nil, message)
}

func (s *Sender) SendWithOptions(ctx context.Context, endpoint, method string, headers map[string]string, message notification.Message) error {
	if s == nil || s.client == nil {
		return errors.New("custom webhook sender is not initialized")
	}
	method = strings.ToUpper(strings.TrimSpace(method))
	if method == "" {
		method = http.MethodPost
	}
	return notification.SendMessage(ctx, s.client, method, endpoint, headers, message)
}
