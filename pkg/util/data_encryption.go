package util

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
)

const dataCiphertextPrefix = "fnsdb:v1:"

var (
	// ErrDataEncryptionKeyRequired is returned when protected database data is
	// used without database.data-encryption-key being configured.
	ErrDataEncryptionKeyRequired = errors.New("database.data-encryption-key is required")
	ErrDataEncryptionKeyInsecure = errors.New("database.data-encryption-key must be a unique random key of at least 32 bytes, not the example key")
	// ErrInvalidDataCiphertext is returned when a value marked as encrypted
	// cannot be decoded or authenticated.
	ErrInvalidDataCiphertext = errors.New("invalid encrypted database value")
)

// DataEncryptor encrypts sensitive values stored in user databases.
//
// The configured value is hashed to an AES-256 key so deployments may use a
// normal high-entropy string without having to pre-format it as a raw key.
// AES-GCM provides confidentiality and authenticity; a fresh nonce is used for
// every value written.
type DataEncryptor struct {
	aead cipher.AEAD
}

// NewDataEncryptor creates an AES-GCM data encryptor from an operator key.
func NewDataEncryptor(key string) (*DataEncryptor, error) {
	if strings.TrimSpace(key) == "" {
		return nil, ErrDataEncryptionKeyRequired
	}

	if len(strings.TrimSpace(key)) < 32 || strings.TrimSpace(key) == "fast-note-sync-database-encryption" {
		return nil, ErrDataEncryptionKeyInsecure
	}

	derivedKey := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(derivedKey[:])
	if err != nil {
		return nil, fmt.Errorf("create database data cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create database data AEAD: %w", err)
	}
	return &DataEncryptor{aead: aead}, nil
}

// IsEncryptedData reports whether value uses the current database ciphertext
// envelope.
func IsEncryptedData(value string) bool {
	return strings.HasPrefix(value, dataCiphertextPrefix)
}

// Encrypt returns an authenticated ciphertext. Empty values remain empty so
// optional credentials retain their existing database representation.
func (e *DataEncryptor) Encrypt(plaintext string) (string, error) {
	if e == nil || e.aead == nil {
		return "", ErrDataEncryptionKeyRequired
	}
	if plaintext == "" {
		return "", nil
	}

	nonce := make([]byte, e.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate database data nonce: %w", err)
	}
	ciphertext := e.aead.Seal(nil, nonce, []byte(plaintext), []byte(dataCiphertextPrefix))
	payload := append(nonce, ciphertext...)
	return dataCiphertextPrefix + base64.RawStdEncoding.EncodeToString(payload), nil
}

// Decrypt returns plaintext from the current authenticated ciphertext format.
// Non-empty values that are not encrypted are rejected so runtime repository
// reads maintain the database ciphertext invariant.
func (e *DataEncryptor) Decrypt(value string) (plaintext string, err error) {
	if value == "" {
		return "", nil
	}
	if !IsEncryptedData(value) {
		return "", ErrInvalidDataCiphertext
	}
	if e == nil || e.aead == nil {
		return "", ErrDataEncryptionKeyRequired
	}

	payload, decodeErr := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(value, dataCiphertextPrefix))
	if decodeErr != nil || len(payload) < e.aead.NonceSize() {
		return "", ErrInvalidDataCiphertext
	}
	nonce := payload[:e.aead.NonceSize()]
	sealed := payload[e.aead.NonceSize():]
	plaintextBytes, openErr := e.aead.Open(nil, nonce, sealed, []byte(dataCiphertextPrefix))
	if openErr != nil {
		return "", ErrInvalidDataCiphertext
	}
	return string(plaintextBytes), nil
}

// GenerateDataEncryptionKey generates 256 bits of entropy without a weak RNG fallback.
func GenerateDataEncryptionKey() (string, error) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return "", fmt.Errorf("generate secret key: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(key), nil
}
