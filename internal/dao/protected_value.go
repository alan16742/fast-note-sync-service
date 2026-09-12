package dao

import "github.com/haierkeys/fast-note-sync-service/pkg/util"

// ValueTransformer transforms one persisted string value. Business
// repositories explicitly select the fields passed to transformStrings.
type ValueTransformer func(string) (string, error)

// DataProtector is the repository-facing adapter for sensitive string values.
// It knows only how to delegate encryption/decryption and does not know any
// business model or field names.
type DataProtector struct {
	encryptor *util.DataEncryptor
}

// NewDataProtector creates a repository data protector.
func NewDataProtector(encryptor *util.DataEncryptor) *DataProtector {
	return &DataProtector{encryptor: encryptor}
}

// Configured reports whether this protector has a usable underlying cipher.
// Startup and migrations use it to fail closed even when there are currently
// no non-empty values to transform.
func (p *DataProtector) Configured() bool {
	return p != nil && p.encryptor != nil
}

// Encrypt encrypts a non-empty value and preserves empty-value semantics.
func (p *DataProtector) Encrypt(plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	if p == nil || p.encryptor == nil {
		return "", util.ErrDataEncryptionKeyRequired
	}
	return p.encryptor.Encrypt(plaintext)
}

// Decrypt strictly decrypts a non-empty value and preserves empty-value
// semantics. Non-empty plaintext is rejected by the underlying encryptor.
func (p *DataProtector) Decrypt(ciphertext string) (string, error) {
	if ciphertext == "" {
		return "", nil
	}
	if p == nil || p.encryptor == nil {
		return "", util.ErrDataEncryptionKeyRequired
	}
	return p.encryptor.Decrypt(ciphertext)
}

// transformStrings applies a transformation to explicitly declared string
// fields. It intentionally does not use reflection or struct tags.
func transformStrings(transform ValueTransformer, values ...*string) error {
	if transform == nil {
		return util.ErrDataEncryptionKeyRequired
	}
	for _, value := range values {
		if value == nil {
			continue
		}
		transformed, err := transform(*value)
		if err != nil {
			return err
		}
		*value = transformed
	}
	return nil
}

// cloneAndTransform shallow-copies a model before transforming its declared
// sensitive string fields. Current protected fields are strings; if a future
// protected field is stored in a map/slice/pointer, its transform must perform
// an appropriate deep copy before mutation.
func cloneAndTransform[T any](src *T, transform func(*T) error) (*T, error) {
	if src == nil {
		return nil, nil
	}
	dst := *src
	if err := transform(&dst); err != nil {
		return nil, err
	}
	return &dst, nil
}
