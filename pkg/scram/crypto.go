package scram

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/xdg-go/stringprep"
	"golang.org/x/text/unicode/norm"
)

// This file implements the cryptographic primitives of RFC 5802, the building
// blocks of SCRAM-SHA-256. They are unexported: the mechanism is the package's
// API, and a caller with a use for the primitives themselves wants a crypto
// library, not this one.

// hmacSHA256 computes the HMAC-SHA256 of msg under key. It is RFC 5802's HMAC:
//
//	Apply the HMAC keyed hash algorithm (defined in RFC 2104) using the octet
//	string represented by "key" as the key and the octet string "msg" as the
//	input string. The size of the result is the hash result size for the hash
//	function in use. For SHA-256, this is 32 octets.
func hmacSHA256(key, msg []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(msg)
	return mac.Sum(nil)
}

// hi derives a key from a password, salt and iteration count. It is RFC 5802's
// Hi, which is PBKDF2 with HMAC as the pseudorandom function and a derived key
// the length of one HMAC output:
//
//	U1   := HMAC(str, salt + INT(1))
//	U2   := HMAC(str, U1)
//	...
//	Ui-1 := HMAC(str, Ui-2)
//	Ui   := HMAC(str, Ui-1)
//
//	Hi := U1 XOR U2 XOR ... XOR Ui
//
// Where "i" is the iteration count, "+" is the string concatenation operator,
// and INT(g) is a 4-octet encoding of the integer g, most significant octet
// first.
func hi(password, salt []byte, iterations int) []byte {
	var ui, uPrev []byte
	ui = hmacSHA256(password, append(salt, 0, 0, 0, 1))
	uPrev = ui

	for i := 1; i < iterations; i++ {
		uPrev = hmacSHA256(password, uPrev)
		for j := range ui {
			ui[j] ^= uPrev[j]
		}
	}
	return ui
}

// hash computes the SHA-256 of data. It is RFC 5802's H:
//
//	Apply the cryptographic hash function to the octet string "data", producing
//	an octet string as a result. The size of the result depends on the hash
//	result size for the hash function in use. For SHA-256, this is 32 octets.
func hash(data string) []byte {
	sum := sha256.New()
	sum.Write([]byte(data))
	return sum.Sum(nil)
}

// normalize follows PostgreSQL's SASLprep behavior, retaining the original bytes
// when UTF-8 validation or preparation fails.
func normalize(data string) string {
	if !utf8.ValidString(data) {
		return data
	}

	// PostgreSQL maps U+200B to a space before the overlapping map-to-nothing
	// rule, and checks prohibited/unassigned characters and bidi before NFKC.
	profile := stringprep.SASLprep
	profile.Normalize = false
	prepared, err := profile.Prepare(strings.ReplaceAll(data, "\u200b", " "))
	if err != nil || prepared == "" {
		return data
	}
	return norm.NFKC.String(prepared)
}

// genSalt returns size cryptographically secure random bytes, for a salt or a
// nonce.
func genSalt(size int) ([]byte, error) {
	salt := make([]byte, size)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, err
	}
	return salt, nil
}

// encodeB64 encodes src to standard base64, pre-allocating the destination.
func encodeB64(src []byte) (dst []byte) {
	dst = make([]byte, base64.StdEncoding.EncodedLen(len(src)))
	base64.StdEncoding.Encode(dst, src)
	return
}
