package api

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/bogie5464/netflow-collector/internal/config"
)

// CheckConfig rejects a configuration that would serve /v1 without a key.
// An empty NFC_API_KEYS is a configuration error, never "no key required":
// that is how an internal service ends up open, silently and permanently.
func CheckConfig(cfg config.Config) error {
	if cfg.HTTPAddr == "" {
		return nil // API disabled
	}
	if len(cfg.APIKeys) == 0 {
		return errors.New("NFC_API_KEYS: must list at least one key while NFC_HTTP_ADDR enables the API")
	}
	for i, k := range cfg.APIKeys {
		if strings.TrimSpace(k) == "" {
			return fmt.Errorf("NFC_API_KEYS: key %d is empty", i+1)
		}
	}
	return nil
}

// requireAPIKey is the /v1 subtree middleware. It reads
// Authorization: Bearer <key> and accepts the request only when the token
// equals one of keys. Missing header, malformed header, empty key and
// unknown key all produce the same 401 so the response never says which.
func requireAPIKey(keys []string, log *slog.Logger, next http.Handler) http.Handler {
	byteKeys := make([][]byte, len(keys))
	for i, k := range keys {
		byteKeys[i] = []byte(k)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok || !keyMatches(byteKeys, []byte(token)) {
			log.Warn("unauthorized", "request_id", RequestID(r.Context()), "path", r.URL.Path,
				"key_prefix", prefix(token), "header_present", r.Header.Get("Authorization") != "")
			w.Header().Set("WWW-Authenticate", `Bearer realm="netflow-collector"`)
			writeError(w, r, http.StatusUnauthorized, CodeUnauthorized, "a valid API key is required", nil)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// keyMatches compares token against every configured key with
// crypto/subtle.ConstantTimeCompare and accumulates the result: it never
// returns early, so timing does not reveal which key matched or how far
// down the list the comparison got.
func keyMatches(keys [][]byte, token []byte) bool {
	matched := 0
	for _, k := range keys {
		matched |= subtle.ConstantTimeCompare(k, token)
	}
	return matched == 1
}

func bearerToken(header string) (string, bool) {
	const scheme = "Bearer "
	if len(header) <= len(scheme) || !strings.EqualFold(header[:len(scheme)], scheme) {
		return "", false
	}
	token := strings.TrimSpace(header[len(scheme):])
	return token, token != ""
}

// prefix is the only part of a presented key that ever reaches a log line.
func prefix(token string) string {
	if len(token) > 6 {
		return token[:6]
	}
	return token
}
