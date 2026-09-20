// Package migrations embeds every backend's goose migrations. This file must
// stay at the root of migrations/: a //go:embed directive resolves relative to
// its own directory and cannot reference a parent, so it cannot live inside
// internal/sink/*. An empty directory fails to embed, so a backend's directory
// and its first migration land together.
package migrations

import (
	"embed"
	"sync"
)

// FS holds <backend>/<NNNNN>_<name>.sql for every backend.
//
//go:embed postgres
var FS embed.FS

// Lock serialises Migrate calls. goose's base FS and dialect are process-wide
// settings, so two backends migrating at once would race on them.
var Lock sync.Mutex
