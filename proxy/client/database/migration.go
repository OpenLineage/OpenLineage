package database

import (
	"database/sql"
	"fmt"
	"github.com/OpenLineage/OpenLineage/client-proxy/config"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/mattn/go-sqlite3"
)

// Migrate migrates the sqlite3 db schema from a source path in config.
func Migrate(conf config.Config) error {
	sourceURL := fmt.Sprintf("file://%s", conf.DatabaseMigrationSource)

	db, err := sql.Open("sqlite3", conf.SqliteDatabasePath)
	if err != nil {
		return err
	}

	driver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
	if err != nil {
		return err
	}

	m, err := migrate.NewWithDatabaseInstance(sourceURL, "ql", driver)
	if err != nil {
		return err
	}

	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		return err
	}

	return db.Close()
}
