// Package rand provides random string generation utilities.
package rand

import "crypto/rand"

var charset = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// ID generates a random alphanumeric string of length n.
func ID(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	for i, v := range b {
		b[i] = charset[int(v)%len(charset)]
	}

	return string(b), nil
}
