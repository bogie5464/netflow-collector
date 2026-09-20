// Command collector is the NetFlow Collector daemon.
//
// Exit codes are a public interface: 0 clean, 1 unexpected error, 2 invalid
// configuration (the message names the variable), 3 migration failure at boot.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/bogie5464/netflow-collector/internal/app"
	"github.com/bogie5464/netflow-collector/internal/config"
)

// version is stamped at build time with
// -ldflags "-X main.version=v1.2.3"; a plain `go build` reports dev.
var version = "dev"

const (
	exitOK        = 0
	exitError     = 1
	exitConfig    = 2
	exitMigration = 3
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("collector", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	showVersion := fs.Bool("version", false, "print the version and exit")
	validateConfig := fs.Bool("validate-config", false, "load and validate configuration, then exit")
	dumpOpenAPI := fs.Bool("dump-openapi", false, "write the OpenAPI document to stdout and exit")
	healthcheck := fs.Bool("healthcheck", false, "probe the running collector's /healthz and exit")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitConfig
	}

	// These short-circuit before configuration validation so they keep working
	// as later steps tighten what a full boot requires.
	switch {
	case *showVersion:
		fmt.Printf("netflow-collector %s\n", version)
		return exitOK
	case *dumpOpenAPI:
		if _, err := os.Stdout.Write(app.OpenAPI()); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return exitError
		}
		return exitOK
	case *healthcheck:
		return runHealthcheck()
	}

	if *validateConfig {
		cfg, err := config.Load()
		if err == nil {
			err = app.CheckNames(*cfg)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return exitConfig
		}
		fmt.Printf("configuration ok: sources=%v sinks=%v\n", cfg.Sources, cfg.Sinks)
		return exitOK
	}

	err := app.Run(context.Background())
	switch {
	case err == nil:
		return exitOK
	case errors.Is(err, app.ErrConfig):
		fmt.Fprintln(os.Stderr, err)
		return exitConfig
	case errors.Is(err, app.ErrMigration):
		fmt.Fprintln(os.Stderr, err)
		return exitMigration
	default:
		fmt.Fprintln(os.Stderr, err)
		return exitError
	}
}

// runHealthcheck GETs /healthz on the local API port and exits 0 on 200,
// 1 otherwise. It is the container healthcheck: the distroless image has no
// shell and no curl, so the binary checks itself. It reads NFC_HTTP_ADDR
// directly because it must work before, and independently of, full
// configuration validation — the one deliberate exception to the rule that
// only internal/config reads the environment.
func runHealthcheck() int {
	port := "8080"
	if addr := os.Getenv("NFC_HTTP_ADDR"); addr != "" {
		if _, p, err := net.SplitHostPort(addr); err == nil && p != "" {
			port = p
		}
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		fmt.Fprintln(os.Stderr, "collector: healthcheck:", err)
		return exitError
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "collector: healthcheck: /healthz returned %d\n", resp.StatusCode)
		return exitError
	}
	return exitOK
}
