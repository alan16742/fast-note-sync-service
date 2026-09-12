package util

import "testing"

func TestPasswordHashUsesArgon2id(t *testing.T) {
	hash, err := GeneratePasswordHash("correct horse battery staple")
	if err != nil {
		t.Fatalf("GeneratePasswordHash() error = %v", err)
	}
	if !IsArgon2idHash(hash) {
		t.Fatalf("hash = %q, want Argon2id PHC format", hash)
	}
	if !CheckPasswordHash(hash, "correct horse battery staple") {
		t.Fatal("CheckPasswordHash() rejected the original password")
	}
	if CheckPasswordHash(hash, "wrong password") {
		t.Fatal("CheckPasswordHash() accepted the wrong password")
	}
}

func TestPasswordHashRetainsBcryptCompatibility(t *testing.T) {
	bcryptHash := "$2a$10$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi"
	if !CheckPasswordHash(bcryptHash, "password") {
		t.Fatal("bcrypt password should remain verifiable during migration")
	}
}

func TestPasswordHashRejectsMalformedOrExcessiveParameters(t *testing.T) {
	for _, parameters := range []string{"m=1048576,t=3,p=2", "m=65536,t=10,p=2", "m=65536,m=65536,p=2", "m=65536,t=3,p=2,unknown=1", "m=+65536,t=3,p=2", "m=65536,t=3,p=0"} {
		hash := "$argon2id$v=19$" + parameters + "$MDEyMzQ1Njc4OWFiY2RlZg$MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY"
		if CheckPasswordHash(hash, "password") {
			t.Fatalf("accepted invalid parameters %s", parameters)
		}
	}
}
