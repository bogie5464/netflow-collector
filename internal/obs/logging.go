package obs

import (
	"io"
	"log/slog"
	"strings"
)

// Redacted replaces the value of any attribute whose key is on the
// redaction list. Redaction lives in the logger, never at the call site, so
// a future config-dump or request-dump helper cannot reintroduce a leak.
const Redacted = "[redacted]"

// redactedKeys are matched case-insensitively against the attribute key,
// as an exact match or as a suffix after "_" or "." (so nfc_api_keys and
// NFC_POSTGRES_DSN are covered along with the bare names).
var redactedKeys = []string{"authorization", "api_key", "api_keys", "dsn", "password", "token", "secret"}

// SetupLogging installs a single-line JSON slog handler at level on w as
// the process default and returns it. Every line carries time, level, msg
// and service.
func SetupLogging(w io.Writer, level, service string) *slog.Logger {
	logger := slog.New(NewHandler(w, ParseLevel(level))).With("service", service)
	slog.SetDefault(logger)
	return logger
}

// NewHandler is the JSON handler with redaction, exposed for tests.
func NewHandler(w io.Writer, level slog.Level) slog.Handler {
	return slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level, ReplaceAttr: redact})
}

// ParseLevel maps NFC_LOG_LEVEL onto a slog level; unknown values mean info.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func redact(_ []string, a slog.Attr) slog.Attr {
	if a.Value.Kind() == slog.KindGroup {
		return a // members are visited individually
	}
	if IsRedactedKey(a.Key) {
		return slog.String(a.Key, Redacted)
	}
	return a
}

// IsRedactedKey reports whether an attribute with this key must be redacted.
func IsRedactedKey(key string) bool {
	k := strings.ToLower(key)
	for _, r := range redactedKeys {
		if k == r || strings.HasSuffix(k, "_"+r) || strings.HasSuffix(k, "."+r) {
			return true
		}
	}
	return false
}
