package util

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/bcrypt"
)

const (
	passwordHashAlgorithm = "argon2id"
	passwordHashVersion   = 19
	passwordMemoryKiB     = 64 * 1024
	passwordIterations    = 3
	passwordParallelism   = 2
	passwordSaltBytes     = 16
	passwordKeyBytes      = 32
)

// GeneratePasswordHash generates an Argon2id password hash in PHC format.
// GeneratePasswordHash 生成 Argon2id PHC 格式密码哈希值
// password: original password string // 原始密码字符串
// return: hashed password string, and possible error info // 返回值: 哈希后的密码字符串，以及可能的错误信息
func GeneratePasswordHash(password string) (string, error) {
	salt := make([]byte, passwordSaltBytes)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	hash := argon2.IDKey([]byte(password), salt, passwordIterations, passwordMemoryKiB, passwordParallelism, passwordKeyBytes)
	encode := base64.RawStdEncoding.EncodeToString
	return fmt.Sprintf("$%s$v=%d$m=%d,t=%d,p=%d$%s$%s",
		passwordHashAlgorithm,
		passwordHashVersion,
		passwordMemoryKiB,
		passwordIterations,
		passwordParallelism,
		encode(salt),
		encode(hash),
	), nil
}

// CheckPasswordHash verifies whether password matches the hash
// CheckPasswordHash 验证密码与哈希值是否匹配
// hash: stored hash value // 存储的哈希值
// password: password to be verified // 待验证的密码
// return: true if password matches, false otherwise // 返回值: 如果密码匹配返回true，否则返回false
func CheckPasswordHash(hash, password string) bool {
	if IsArgon2idHash(hash) {
		return checkArgon2idHash(hash, password)
	}
	// Existing installations used bcrypt before the password-storage policy
	// was standardized on Argon2id. Continue accepting those hashes so an
	// upgrade does not lock users out; all newly generated hashes are Argon2id.
	if strings.HasPrefix(hash, "$2a$") || strings.HasPrefix(hash, "$2b$") || strings.HasPrefix(hash, "$2y$") {
		return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
	}
	return false
}

// IsArgon2idHash reports whether hash uses the current password format.
func IsArgon2idHash(hash string) bool {
	return strings.HasPrefix(hash, "$argon2id$")
}

func checkArgon2idHash(encoded, password string) bool {
	if len(encoded) > 512 {
		return false
	}
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[1] != passwordHashAlgorithm {
		return false
	}
	if len(strings.Split(parts[3], ",")) != 3 || strings.Contains(parts[2], ",") {
		return false
	}
	version, ok := parsePasswordParameter(parts[2], "v")
	if !ok || version != passwordHashVersion {
		return false
	}
	memory, ok := parsePasswordParameter(parts[3], "m")
	if !ok || memory < 8*1024 || memory > 256*1024 {
		return false
	}
	iterations, ok := parsePasswordParameter(parts[3], "t")
	if !ok || iterations < 1 || iterations > 6 {
		return false
	}
	parallelism, ok := parsePasswordParameter(parts[3], "p")
	if !ok || parallelism < 1 || parallelism > 16 {
		return false
	}
	decode := base64.RawStdEncoding
	salt, err := decode.DecodeString(parts[4])
	if err != nil || len(salt) < 8 || len(salt) > 64 {
		return false
	}
	expected, err := decode.DecodeString(parts[5])
	if err != nil || len(expected) == 0 || len(expected) > 64 {
		return false
	}
	actual := argon2.IDKey([]byte(password), salt, uint32(iterations), uint32(memory), uint8(parallelism), uint32(len(expected)))
	return subtle.ConstantTimeCompare(actual, expected) == 1
}

func parsePasswordParameter(parameters, name string) (int, bool) {
	var result int
	found := false
	for _, parameter := range strings.Split(parameters, ",") {
		key, value, ok := strings.Cut(parameter, "=")
		if !ok || key != name {
			continue
		}
		if found || value == "" || strings.IndexFunc(value, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
			return 0, false
		}
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return 0, false
		}
		result, found = parsed, true
	}
	return result, found
}
