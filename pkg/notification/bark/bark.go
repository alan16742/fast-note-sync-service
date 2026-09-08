package bark

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/haierkeys/fast-note-sync-service/pkg/notification"
)

const DefaultEndpoint = "https://api.day.app"

// Endpoint accepts the server base address; the device key belongs in the body.
func Endpoint(endpoint string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}
	base, err := url.Parse(endpoint)
	if err != nil || base.Host == "" || (base.Scheme != "http" && base.Scheme != "https") || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return "", errors.New("invalid Bark server URL: use a base URL without a key, query or fragment")
	}
	return strings.TrimRight(base.String(), "/") + "/push", nil
}

type Sender struct{ client *http.Client }

func NewClient(client *http.Client) *Sender {
	return &Sender{client: notification.HTTPClient(client)}
}

func (s *Sender) Send(ctx context.Context, endpoint, credential string, message notification.Message) error {
	if strings.TrimSpace(credential) == "" {
		return errors.New("bark device key is required")
	}
	if s == nil || s.client == nil {
		return errors.New("bark sender is not initialized")
	}
	endpoint, err := Endpoint(endpoint)
	if err != nil {
		return err
	}
	payload := struct {
		DeviceKey string `json:"device_key"`
		Title     string `json:"title"`
		Body      string `json:"body"`
		Group     string `json:"group,omitempty"`
		URL       string `json:"url,omitempty"`
		Level     string `json:"level,omitempty"`
	}{strings.TrimSpace(credential), message.Title, message.Body, message.Group, message.URL, message.Level}
	return notification.PostJSON(ctx, s.client, endpoint, payload, 200)
}
