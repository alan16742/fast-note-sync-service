package dao

import (
	"errors"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestDataProtectorEncryptsAndDecryptsValues(t *testing.T) {
	encryptor, err := util.NewDataEncryptor("protector test key-fixture-0123456789abcdef0123456789abcdef")
	require.NoError(t, err)
	protector := NewDataProtector(encryptor)
	require.True(t, protector.Configured())

	ciphertext, err := protector.Encrypt("secret")
	require.NoError(t, err)
	require.NotEqual(t, "secret", ciphertext)
	plaintext, err := protector.Decrypt(ciphertext)
	require.NoError(t, err)
	require.Equal(t, "secret", plaintext)
	empty, err := protector.Encrypt("")
	require.NoError(t, err)
	require.Empty(t, empty)
	empty, err = protector.Decrypt("")
	require.NoError(t, err)
	require.Empty(t, empty)
	_, err = protector.Decrypt("plain-text")
	require.ErrorIs(t, err, util.ErrInvalidDataCiphertext)
}

func TestTransformStringsAndCloneAndTransform(t *testing.T) {
	first, second := "first", "second"
	err := transformStrings(func(value string) (string, error) { return "enc:" + value, nil }, &first, &second, nil)
	require.NoError(t, err)
	require.Equal(t, "enc:first", first)
	require.Equal(t, "enc:second", second)

	failure := errors.New("transform failed")
	err = transformStrings(func(string) (string, error) { return "", failure }, &first)
	require.ErrorIs(t, err, failure)

	type protected struct{ Value string }
	original := &protected{Value: "ciphertext"}
	copyValue, err := cloneAndTransform(original, func(value *protected) error {
		value.Value = "plaintext"
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, "ciphertext", original.Value)
	require.Equal(t, "plaintext", copyValue.Value)
}
