/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package file

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// FileRecord is the structure written to disk for each OpenLineage job.
// It wraps the raw OL RunEvent with metadata useful for auditing and debugging.
//
// Using a versioned wrapper (SchemaVersion) means the on-disk format can evolve
// without breaking existing files — a practice worth carrying into real providers
// that store state in external systems.
type FileRecord struct {
	// SchemaVersion allows future migrations of the on-disk format.
	SchemaVersion string `json:"schema_version"`

	// Job identifies which OpenLineage job this record belongs to.
	Job JobRef `json:"job"`

	// LastEmitted is the RFC3339 UTC timestamp of the most recent write.
	LastEmitted string `json:"last_emitted"`

	// EmitCount tracks how many times this file has been written.
	// Starts at 1 on creation, increments on each update.
	// Stored here so ConsumerRead() can recover state even after `terraform state rm`.
	EmitCount int64 `json:"emit_count"`

	// RunID is the UUID of the most recent OL RunEvent.
	RunID string `json:"run_id"`

	// Event is the raw OpenLineage RunEvent JSON exactly as it would be sent
	// to a real backend (e.g. Marquez HTTP API, Dataplex gRPC).
	// json.RawMessage embeds the pre-serialised bytes verbatim — no double-encoding.
	Event json.RawMessage `json:"event"`
}

// JobRef is the minimal job identifier embedded in FileRecord.
type JobRef struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// FilePath returns the canonical on-disk path for a lineage file.
//
//	{outputDir}/{prefix}__{namespace}__{name}.json
//
// prefix is "run", "job", or "dataset" — prevents name collisions when the
// same namespace+name is used across multiple resource types.
// The double-underscore separator is chosen because namespace and name may
// each contain slashes, dots, or single underscores. Slashes are replaced
// with dashes to keep the result a valid single path component.
func FilePath(outputDir, prefix, namespace, name string) string {
	sanitise := func(s string) string {
		return strings.NewReplacer("/", "-", "\\", "-").Replace(s)
	}
	filename := fmt.Sprintf("%s__%s__%s.json", prefix, sanitise(namespace), sanitise(name))
	return filepath.Join(outputDir, filename)
}

// Write serialises rec to path.
//
// The write is atomic: the data is first written to a sibling temp file,
// then renamed into place. This prevents a concurrent `terraform show` or
// external reader from seeing a partially-written file.
func Write(path string, rec *FileRecord, prettyPrint bool) error {
	var (
		data []byte
		err  error
	)
	if prettyPrint {
		data, err = json.MarshalIndent(rec, "", "  ")
	} else {
		data, err = json.Marshal(rec)
	}
	if err != nil {
		return fmt.Errorf("marshal file record: %w", err)
	}

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".lineage-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()

	if _, err = tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("write temp file: %w", err)
	}
	if err = tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close temp file: %w", err)
	}
	if err = os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename temp file to destination: %w", err)
	}
	return nil
}

// Read deserialises the FileRecord at path.
// Returns (nil, nil) if the file does not exist — the caller interprets nil
// as "resource no longer present" and triggers re-create, exactly as
// Dataplex's getProcess() returning nil triggers re-create.
func Read(path string) (*FileRecord, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read lineage file: %w", err)
	}

	var rec FileRecord
	if err = json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal lineage file: %w", err)
	}
	return &rec, nil
}

// Delete removes the lineage file at path.
// Returns nil if the file does not exist — Delete is idempotent,
// matching the contract expected by ConsumerDelete.
func Delete(path string) error {
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// now returns the current UTC time formatted as RFC3339.
// Extracted to a package-level variable so tests can substitute a fixed clock.
var now = func() string {
	return time.Now().UTC().Format(time.RFC3339)
}
