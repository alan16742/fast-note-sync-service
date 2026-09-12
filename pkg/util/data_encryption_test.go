package util

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestDataEncryptorRoundTripAndRandomNonce(t *testing.T) {
	encryptor, err := NewDataEncryptor("test database key-fixture-0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatalf("NewDataEncryptor() error = %v", err)
	}

	first, err := encryptor.Encrypt("secret value")
	if err != nil {
		t.Fatalf("Encrypt() error = %v", err)
	}
	second, err := encryptor.Encrypt("secret value")
	if err != nil {
		t.Fatalf("Encrypt() second error = %v", err)
	}
	if first == second {
		t.Fatal("Encrypt() reused ciphertext; expected a fresh nonce")
	}

	plaintext, err := encryptor.Decrypt(first)
	if err != nil || plaintext != "secret value" {
		t.Fatalf("Decrypt() = (%q, %v), want authenticated round trip", plaintext, err)
	}
}

func TestDataEncryptorRejectsPlaintextAndWrongKey(t *testing.T) {
	encryptor, err := NewDataEncryptor("test database key-fixture-0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatalf("NewDataEncryptor() error = %v", err)
	}
	if _, err := encryptor.Decrypt("unexpected plaintext"); err != ErrInvalidDataCiphertext {
		t.Fatalf("plaintext Decrypt() error = %v, want ErrInvalidDataCiphertext", err)
	}

	ciphertext, err := encryptor.Encrypt("secret value")
	if err != nil {
		t.Fatalf("Encrypt() error = %v", err)
	}
	wrongKey, err := NewDataEncryptor("different database key-fixture-0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatalf("NewDataEncryptor(wrong key) error = %v", err)
	}
	if _, err := wrongKey.Decrypt(ciphertext); err != ErrInvalidDataCiphertext {
		t.Fatalf("wrong-key Decrypt() error = %v, want ErrInvalidDataCiphertext", err)
	}
}

func TestNewDataEncryptorRequiresConfiguredKey(t *testing.T) {
	if _, err := NewDataEncryptor(" "); err != ErrDataEncryptionKeyRequired {
		t.Fatalf("NewDataEncryptor() error = %v, want ErrDataEncryptionKeyRequired", err)
	}
	if _, err := NewDataEncryptor("configured-fixture-0123456789abcdef0123456789abcdef"); err != nil {
		t.Fatalf("NewDataEncryptor() configured error = %v", err)
	}
}

func TestSensitiveHeaderClassification(t *testing.T) {
	for _, name := range []string{"Authorization", "X-Auth-Token", "X-Api-Key", "X-ApiKey", "X-Secret", "Cookie", "X-Key"} {
		if !IsSensitiveHeaderName(name) {
			t.Fatalf("IsSensitiveHeaderName(%q) = false", name)
		}
	}
	if IsSensitiveHeaderName("X-Source") {
		t.Fatal("ordinary source header must not be classified as sensitive")
	}
	for _, name := range []string{"token", "api_key", "apiKey", "secret"} {
		if !IsSensitiveURLQueryName(name) {
			t.Fatalf("IsSensitiveURLQueryName(%q) = false", name)
		}
	}
}

func TestDataEncryptorRejectsWeakKeysAndTamperedData(t *testing.T) {
	for _, key := range []string{"short", "fast-note-sync-database-encryption", " fast-note-sync-database-encryption "} {
		if _, err := NewDataEncryptor(key); err != ErrDataEncryptionKeyInsecure {
			t.Fatalf("expected weak key rejection, got %v", err)
		}
	}
	key, err := GenerateDataEncryptionKey()
	if err != nil {
		t.Fatal(err)
	}
	e, err := NewDataEncryptor(key)
	if err != nil {
		t.Fatal(err)
	}
	value, err := e.Encrypt("confidential")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(value, dataCiphertextPrefix))
	if err != nil {
		t.Fatal(err)
	}
	payload[len(payload)-1] ^= 1
	if _, err := e.Decrypt(dataCiphertextPrefix + base64.RawStdEncoding.EncodeToString(payload)); err != ErrInvalidDataCiphertext {
		t.Fatalf("tampered data accepted: %v", err)
	}
}
