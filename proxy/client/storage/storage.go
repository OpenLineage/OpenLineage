// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"database/sql"
	"fmt"
	"github.com/OpenLineage/OpenLineage/client-proxy/config"
	_ "github.com/mattn/go-sqlite3"
	"math/rand"
	"time"
)

type Storage struct {
	db *sql.DB
}

type Partition struct {
	RowID     uint64
	Name      string
	Size      uint64
	CreatedAt time.Time
	IsCurrent bool
}

// IStorage is an interface for interaction with local storage database
type IStorage interface {
	GetCurrentPartition() (*Partition, error)
	RotatePartition() (*Partition, error)
	InsertLineageEvent(*Partition, string) error
}

// GetCurrentPartition returns the `Partition` where its `isCurrent` is true if available. Otherwise,
// sql.ErrNoRows will be returned.
func (s *Storage) GetCurrentPartition() (*Partition, error) {
	return getCurrentPartition(s.db)
}

// RotatePartition rotates the lineage events partitions when the current partition hits the bytes or
// hours limits. It marks the current partition is done for adding more event records, creates a new
// lineage events partition table, and creates a new record in partitions table.
func (s *Storage) RotatePartition() (*Partition, error) {
	p, err := getCurrentPartition(s.db)
	if err != nil {
		return nil, err
	}

	err = inTransaction(s.db, func(tx *sql.Tx) error {
		if _, err := tx.Exec(fmt.Sprintf("INSERT INTO %s(event, continue) VALUES('', false)", p.Name)); err != nil {
			return err
		}

		if _, err := tx.Exec("UPDATE partitions SET is_current=false WHERE name=?", p.Name); err != nil {
			return err
		}

		if err := createPartitionTable(tx); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return getCurrentPartition(s.db)
}

func (s *Storage) InsertLineageEvent(p *Partition, lineageEvent string) error {
	return inTransaction(s.db, func(tx *sql.Tx) error {
		_, err := tx.Exec(fmt.Sprintf("INSERT INTO %s(event, continue) VALUES(?, true)", p.Name), lineageEvent)
		if err != nil {
			return err
		}

		_, err = tx.Exec("UPDATE partitions SET size=size+? WHERE name=?", len(lineageEvent), p.Name)
		if err != nil {
			return err
		}

		return nil
	})
}

// New returns a Storage instance which manages the partitions and checkpoint.
func New(conf config.Config) (*Storage, error) {
	db, err := sql.Open("sqlite3", conf.SqliteDatabasePath)
	if err != nil {
		return nil, err
	}

	if err := initDB(db); err != nil {
		return nil, err
	}

	return &Storage{db: db}, nil
}

func initDB(db *sql.DB) error {
	_, err := getCurrentPartition(db)
	if err == sql.ErrNoRows {
		return inTransaction(db, func(tx *sql.Tx) error {
			return createPartitionTable(tx)
		})
	}

	return err
}

func getCurrentPartition(db *sql.DB) (*Partition, error) {
	var (
		rowID     uint64
		name      string
		size      uint64
		createdAt time.Time
		isCurrent bool
	)

	row := db.QueryRow("SELECT rowid, name, size, created_at, is_current FROM partitions WHERE is_current=true")
	if err := row.Scan(&rowID, &name, &size, &createdAt, &isCurrent); err != nil {
		return nil, err
	}

	return &Partition{RowID: rowID, Name: name, Size: size, CreatedAt: createdAt, IsCurrent: isCurrent}, nil
}

func createPartitionTable(tx *sql.Tx) error {
	tableName := fmt.Sprintf("lineage_events_partition_%s", generatePartitionSuffix())
	_, err := tx.Exec(fmt.Sprintf(`
			CREATE TABLE %s (
			    event    text    NOT NULL,
			    continue boolean NOT NULL
			)
		`, tableName))
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
			INSERT INTO partitions(name, size, created_at, is_current)
			VALUES(?, ?, ?, ?)
		`, tableName, 0, time.Now(), true)
	return nil
}

func generatePartitionSuffix() string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, 15)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func inTransaction(db *sql.DB, transactional func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	if err := transactional(tx); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}
