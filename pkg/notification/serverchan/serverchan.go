package serverchan

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strings"

	"github.com/haierkeys/fast-note-sync-service/pkg/notification"
)

var sc3Key = regexp.MustCompile(`^sctp([0-9]+)t[A-Za-z0-9]+$`)
var turboKey = regexp.MustCompile(`^SCT[A-Za-z0-9]+$`)

// Endpoint derives the current SC3 URL from its UID. Turbo retains its own URL.
func Endpoint(key string) (string, error) {
	if match := sc3Key.FindStringSubmatch(key); match != nil {
		return "https://" + match[1] + ".push.ft07.com/send/" + key + ".send", nil
	}
	if turboKey.MatchString(key) {
		return "https://sctapi.ftqq.com/" + key + ".send", nil
	}
	return "", errors.New("invalid ServerChan SendKey")
}

type Sender struct{ client *http.Client }

func NewClient(client *http.Client) *Sender {
	return &Sender{client: notification.HTTPClient(client)}
}

func (s *Sender) Send(ctx context.Context, _ string, credential string, message notification.Message) error {
	if s == nil || s.client == nil {
		return errors.New("serverchan sender is not initialized")
	}
	endpoint, err := Endpoint(strings.TrimSpace(credential))
	if err != nil {
		return err
	}
	payload := struct {
		Title string `json:"title"`
		Body  string `json:"desp"`
		Short string `json:"short,omitempty"`
		Tags  string `json:"tags,omitempty"`
	}{message.Title, message.Body, message.Short, message.Tags}
	return notification.PostJSON(ctx, s.client, endpoint, payload, 0)
}
