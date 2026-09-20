package main

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// captureStdout redirects the real os.Stdout for the duration of fn. run()
// writes straight to os.Stdout/os.Stderr rather than an injectable writer —
// that IS the process's contract (see the exit-code doc comment on this
// package) — so this is how a test observes it without changing that.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	fn()

	require.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(out)
}

func TestRunVersion(t *testing.T) {
	var code int
	out := captureStdout(t, func() { code = run([]string{"-version"}) })
	require.Equal(t, exitOK, code)
	require.Contains(t, out, "netflow-collector dev")
}

func TestRunHelp(t *testing.T) {
	// -h triggers flag.ErrHelp, which run treats as clean exit, not a
	// configuration error — it must keep working with no environment at all.
	require.Equal(t, exitOK, run([]string{"-h"}))
}

func TestRunUnknownFlag(t *testing.T) {
	require.Equal(t, exitConfig, run([]string{"-this-flag-does-not-exist"}))
}

func TestRunDumpOpenAPI(t *testing.T) {
	var code int
	out := captureStdout(t, func() { code = run([]string{"-dump-openapi"}) })
	require.Equal(t, exitOK, code)
	require.Contains(t, out, `"openapi"`)
}

func TestRunValidateConfig(t *testing.T) {
	t.Run("valid configuration prints ok and exits 0", func(t *testing.T) {
		t.Setenv("NFC_SOURCES", "netflow")
		t.Setenv("NFC_SINKS", "postgres")
		t.Setenv("NFC_NETFLOW_ADDR", "127.0.0.1:2055")
		t.Setenv("NFC_HTTP_ADDR", "")
		t.Setenv("NFC_POSTGRES_DSN", "postgres://netflow:netflow@127.0.0.1:15432/netflow?sslmode=disable")

		var code int
		out := captureStdout(t, func() { code = run([]string{"-validate-config"}) })
		require.Equal(t, exitOK, code)
		require.Contains(t, out, "configuration ok")
	})

	t.Run("invalid configuration exits 2", func(t *testing.T) {
		t.Setenv("NFC_SOURCES", "netflow")
		t.Setenv("NFC_SINKS", "postgres")
		t.Setenv("NFC_NETFLOW_ADDR", "127.0.0.1:2055")
		t.Setenv("NFC_HTTP_ADDR", "")
		t.Setenv("NFC_POSTGRES_DSN", "") // required when postgres is in NFC_SINKS

		require.Equal(t, exitConfig, run([]string{"-validate-config"}))
	})
}

func TestRunHealthcheck(t *testing.T) {
	t.Run("200 from /healthz exits 0", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()
		_, port, err := net.SplitHostPort(srv.Listener.Addr().String())
		require.NoError(t, err)
		t.Setenv("NFC_HTTP_ADDR", ":"+port)

		require.Equal(t, exitOK, run([]string{"-healthcheck"}))
	})

	t.Run("non-200 from /healthz exits 1", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer srv.Close()
		_, port, err := net.SplitHostPort(srv.Listener.Addr().String())
		require.NoError(t, err)
		t.Setenv("NFC_HTTP_ADDR", ":"+port)

		require.Equal(t, exitError, run([]string{"-healthcheck"}))
	})

	t.Run("nothing listening exits 1", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		_, port, err := net.SplitHostPort(l.Addr().String())
		require.NoError(t, err)
		require.NoError(t, l.Close())

		t.Setenv("NFC_HTTP_ADDR", "127.0.0.1:"+port)
		require.Equal(t, exitError, run([]string{"-healthcheck"}))
	})
}

func TestRunDefaultPathIsConfigError(t *testing.T) {
	// No flags: falls through to app.Run(ctx). An invalid environment must
	// surface as exitConfig without ever attempting a database connection.
	t.Setenv("NFC_SOURCES", "netflow")
	t.Setenv("NFC_SINKS", "postgres")
	t.Setenv("NFC_NETFLOW_ADDR", "127.0.0.1:2055")
	t.Setenv("NFC_HTTP_ADDR", "")
	t.Setenv("NFC_POSTGRES_DSN", "")

	require.Equal(t, exitConfig, run(nil))
}
