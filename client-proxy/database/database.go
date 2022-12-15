package database

import (
	"database/sql"
	"github.com/OpenLineage/OpenLineage/client-proxy/config"
	_ "github.com/mattn/go-sqlite3"
	"log"
)

type Database struct {
	db *sql.DB
}

type IDatabase interface {
}

func New(conf config.Config) *Database {
	db, err := sql.Open("sqlite3", conf.SqliteDatabasePath)
	if err != nil {
		log.Fatalln("Cannot open database:", err)
	}

	log.Println("Initializing DB")
	InitDB(db)
	return &Database{db: db}
}

func InitDB(db *sql.DB) {
	db.Exec(`
		CREATE TABLE IF NOT EXISTS partitions (
			name       text     NOT NULL,
			size       integer  NOT NULL,
			created_at datetime NOT NULL,
			current    boolean  NOT NULL
		)
	`)

	db.Exec(`
		CREATE TABLE IF NOT EXISTS checkpoint (
			partition_id integer NOT NULL,
			offset       integer NOT NULL,
			FOREIGN KEY (partition_id) REFERENCES partitions (rowid)
		)
	`)
}
