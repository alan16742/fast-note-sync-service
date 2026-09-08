package notification

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const Timeout = 10 * time.Second
const MaxResponseBytes = 64 * 1024

// HTTPClient bounds requests and prevents credential-bearing redirects.
func HTTPClient(client *http.Client) *http.Client {
	result := http.Client{Timeout: Timeout}
	if client != nil {
		result = *client
		if result.Timeout <= 0 {
			result.Timeout = Timeout
		}
	}
	result.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if result.Transport == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		// Resolve and dial the same validated address, closing the DNS rebinding
		// gap left by checking only the URL's hostname at configuration time.
		transport.Proxy = nil
		transport.DialContext = dialPublicEndpoint
		result.Transport = transport
	}
	return &result
}

func dialPublicEndpoint(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errors.New("invalid notification host")
	}
	addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		return nil, errors.New("notification host has no addresses")
	}
	for _, address := range addresses {
		ip := address.IP
		if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
			return nil, errors.New("notification endpoint must resolve to a public address")
		}
	}
	dialer := &net.Dialer{Timeout: Timeout, KeepAlive: 30 * time.Second}
	for _, address := range addresses {
		connection, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(address.IP.String(), port))
		if dialErr == nil {
			return connection, nil
		}
		err = dialErr
	}
	return nil, err
}

// PostJSON checks HTTP and provider status separately. Errors omit request URLs
// and raw provider responses, both of which may contain credentials.
func PostJSON(ctx context.Context, client *http.Client, endpoint string, payload any, successCode int) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode notification: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return errors.New("invalid notification request")
	}
	req.Header.Set("Content-Type", "application/json;charset=utf-8")
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "fast-note-sync-service-notification/1")
	}
	response, err := client.Do(req)
	if err != nil {
		var requestErr *url.Error
		if errors.As(err, &requestErr) {
			err = requestErr.Err
		}
		return fmt.Errorf("notification request failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("notification HTTP status %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, MaxResponseBytes+1))
	if err != nil {
		return errors.New("read notification response failed")
	}
	if len(data) > MaxResponseBytes {
		return errors.New("notification response too large")
	}
	var result struct {
		Code *int `json:"code"`
	}
	if json.Unmarshal(data, &result) != nil || result.Code == nil {
		return errors.New("invalid notification response: missing numeric code")
	}
	if *result.Code != successCode {
		return fmt.Errorf("notification provider code %d", *result.Code)
	}
	return nil
}

// SendMessage delivers the common notification payload through a user-defined
// HTTP endpoint. GET encodes the payload as query parameters; POST sends JSON.
// Unlike provider-specific APIs, any 2xx response is considered successful.
func SendMessage(ctx context.Context, client *http.Client, method, endpoint string, headers map[string]string, message Message) error {
	method = strings.ToUpper(strings.TrimSpace(method))
	if method != http.MethodGet && method != http.MethodPost {
		return errors.New("custom webhook method must be GET or POST")
	}
	client = HTTPClient(client)
	payload := struct {
		Title string `json:"title,omitempty"`
		Body  string `json:"body,omitempty"`
		Short string `json:"short,omitempty"`
		Tags  string `json:"tags,omitempty"`
		Group string `json:"group,omitempty"`
		URL   string `json:"url,omitempty"`
		Level string `json:"level,omitempty"`
	}{message.Title, message.Body, message.Short, message.Tags, message.Group, message.URL, message.Level}

	var body io.Reader
	if method == http.MethodGet {
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.Host == "" {
			return errors.New("invalid notification request")
		}
		query := parsed.Query()
		query.Set("title", message.Title)
		query.Set("body", message.Body)
		query.Set("short", message.Short)
		query.Set("tags", message.Tags)
		query.Set("group", message.Group)
		query.Set("url", message.URL)
		query.Set("level", message.Level)
		parsed.RawQuery = query.Encode()
		endpoint = parsed.String()
	} else {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("encode notification: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return errors.New("invalid notification request")
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	if method == http.MethodPost && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "fast-note-sync-service-notification/1")
	}
	response, err := client.Do(req)
	if err != nil {
		var requestErr *url.Error
		if errors.As(err, &requestErr) {
			err = requestErr.Err
		}
		return fmt.Errorf("notification request failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("notification HTTP status %d", response.StatusCode)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, MaxResponseBytes))
	return nil
}
