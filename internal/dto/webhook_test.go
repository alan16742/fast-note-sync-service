package dto

import (
	"encoding/json"
	"testing"
)

func TestWebhookSubscriptionRequestUnmarshalAcceptsJSONHeaders(t *testing.T) {
	var request WebhookSubscriptionRequest
	if err := json.Unmarshal([]byte(`{"provider":"custom","headers":{"Authorization":"Bearer token","X-Source":"fns"}}`), &request); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if request.Headers["Authorization"] != "Bearer token" || request.Headers["X-Source"] != "fns" {
		t.Fatalf("headers = %#v, want JSON object", request.Headers)
	}
}
