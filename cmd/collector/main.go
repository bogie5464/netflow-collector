// Command collector is the NetFlow Collector daemon.
//
// Exit codes are a public interface: 0 clean, 1 unexpected error, 2 invalid
// configuration (the message names the variable), 3 migration failure at boot.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

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
		fmt.Fprintln(os.Stderr, "collector: -dump-openapi is not available until the REST API is built")
		return exitError
	case *healthcheck:
		fmt.Fprintln(os.Stderr, "collector: -healthcheck is not available until the ops endpoints are built")
		return exitError
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitConfig
	}
	if *validateConfig {
		fmt.Printf("configuration ok: sources=%v sinks=%v\n", cfg.Sources, cfg.Sinks)
		return exitOK
	}

	// The runtime wiring (internal/app) arrives with the vertical slice.
	fmt.Fprintln(os.Stderr, "collector: runtime not wired yet; use -validate-config")
	return exitError
}

// exitMigration is reserved for internal/app once it owns boot-time migrations.
var _ = exitMigration
