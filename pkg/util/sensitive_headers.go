package util

import "strings"

// IsSensitiveHeaderName identifies header names whose values are normally
// credentials or session material. The check is intentionally case-insensitive
// and also covers provider-specific names such as X-Auth-Token and X-Api-Key.
func IsSensitiveHeaderName(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return false
	}

	for _, exact := range []string{
		"authorization",
		"proxy-authorization",
		"cookie",
		"set-cookie",
		"x-api-key",
		"api-key",
		"apikey",
		"key",
	} {
		if name == exact {
			return true
		}
	}

	for _, marker := range []string{
		"auth",
		"token",
		"secret",
		"password",
		"passwd",
		"credential",
		"api_key",
		"api-key",
		"apikey",
	} {
		if strings.Contains(name, marker) {
			return true
		}
	}
	return isKeyLikeName(name)
}

// IsSensitiveURLQueryName identifies query parameters that commonly carry
// credentials. It is used to redact custom webhook endpoints returned to the
// browser; ordinary template parameters such as title remain visible.
func IsSensitiveURLQueryName(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return false
	}
	for _, marker := range []string{
		"auth",
		"token",
		"secret",
		"password",
		"passwd",
		"credential",
		"api_key",
		"api-key",
		"apikey",
	} {
		if strings.Contains(name, marker) {
			return true
		}
	}
	return isKeyLikeName(name)
}

func isKeyLikeName(name string) bool {
	if name == "key" {
		return true
	}
	for _, suffix := range []string{"-key", "_key"} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return strings.HasPrefix(name, "key-") || strings.HasPrefix(name, "key_")
}
