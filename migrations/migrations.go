// Package migrations embeds the SQL schema migrations, applied in filename order.
package migrations

import "embed"

// FS holds every *.sql migration.
//
//go:embed *.sql
var FS embed.FS
