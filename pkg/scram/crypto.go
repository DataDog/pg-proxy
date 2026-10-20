package scram

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"unicode"

	"golang.org/x/text/unicode/norm"
	"golang.org/x/text/width"
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

// normalize prepares a password for hashing. It stands in for RFC 5802's
// Normalize:
//
//	Apply the SASLprep profile (RFC 4013) of the "stringprep" algorithm (RFC
//	3454) as the normalization algorithm to a UTF-8 encoded "str". The resulting
//	string is also in UTF-8. When applying SASLprep, "str" is treated as a
//	"stored string", which means that unassigned Unicode codepoints are
//	prohibited.
//
// This is a simplified version: it applies NFKC normalization, maps characters
// to their width equivalent, and then drops everything that is not printable
// ASCII. RFC 5802 permits that — an implementation must either implement
// SASLprep or disallow non-US-ASCII codepoints in passwords — but note the
// consequence: a password containing them is not rejected, it is silently
// reduced to the ASCII it does contain.
func normalize(data string) string {
	// Normalize the string using NFKC normalization
	normData := norm.NFKC.String(data)

	// Map the string to its width equivalent
	mappedData := width.Narrow.String(normData)

	// Remove non-ASCII and unassigned Unicode codepoints
	var result []rune
	for _, r := range mappedData {
		if r <= unicode.MaxASCII && unicode.IsPrint(r) {
			result = append(result, r)
		}
	}

	return string(result)
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
