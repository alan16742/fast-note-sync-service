package notification

import "context"

type Message struct {
	Title string
	Body  string
	Short string
	Tags  string
	Group string
	// ObsidianURI is the `obsidian://open?...` link to the note, exposed to
	// templates as {{ob_uri}}. It is the notification link, not the delivery
	// endpoint.
	ObsidianURI string
	Level       string
	// Endpoint is an optional rendered delivery endpoint for custom webhooks.
	Endpoint string
}

type Sender interface {
	Send(ctx context.Context, endpoint, credential string, message Message) error
}

// ConfiguredSender supports delivery options that are part of a custom
// notification channel rather than a provider credential.
type ConfiguredSender interface {
	Sender
	SendWithOptions(ctx context.Context, endpoint, method string, headers map[string]string, message Message) error
}
