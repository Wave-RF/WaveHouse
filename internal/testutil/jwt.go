package testutil

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

// TestJWTSecret is a shared HMAC secret for test JWT generation.
const TestJWTSecret = "test-secret-at-least-32-chars-long!"

// MakeJWT creates a signed HS256 JWT with the given claims merged with standard exp/iat.
func MakeJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	now := time.Now()
	mapClaims := jwt.MapClaims{
		"iat": jwt.NewNumericDate(now),
		"exp": jwt.NewNumericDate(now.Add(time.Hour)),
	}
	for k, v := range claims {
		mapClaims[k] = v
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, mapClaims)
	signed, err := token.SignedString([]byte(TestJWTSecret))
	require.NoError(t, err, "failed to sign test JWT")
	return signed
}

// MakeExpiredJWT creates a signed HS256 JWT that expired 1 hour ago.
func MakeExpiredJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	past := time.Now().Add(-2 * time.Hour)
	mapClaims := jwt.MapClaims{
		"iat": jwt.NewNumericDate(past),
		"exp": jwt.NewNumericDate(past.Add(time.Hour)),
	}
	for k, v := range claims {
		mapClaims[k] = v
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, mapClaims)
	signed, err := token.SignedString([]byte(TestJWTSecret))
	require.NoError(t, err, "failed to sign test JWT")
	return signed
}
