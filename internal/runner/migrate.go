package runner

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
)

//go:embed migrations/001_init.sql
var initialMigration string

func MigratePostgres(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, initialMigration); err != nil {
		return fmt.Errorf("apply postgres migration: %w", err)
	}
	return nil
}
