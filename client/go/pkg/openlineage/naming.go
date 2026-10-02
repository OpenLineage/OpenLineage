/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package openlineage

import (
	"os"
	"strings"
	"sync"
)

const nameEscapingEnvVar = "OPENLINEAGE__NAME__ESCAPING"

// NameConfig holds name-related configuration for the OpenLineage client.
//
// The zero value (all fields nil/false) means "no override; fall back to the
// environment variable OPENLINEAGE__NAME__ESCAPING".
type NameConfig struct {
	// Escaping enables automatic dot-escaping of name segments when true.
	// A nil pointer means "no override; consult the environment variable".
	Escaping *bool
}

var (
	nameConfigMu     sync.RWMutex
	nameConfigGlobal *bool // nil = no override; non-nil = config-derived value
)

// ConfigureName applies the provided NameConfig as the global override used by
// [IsNameEscapingEnabled].
//
// Call this after constructing the client so that a programmatic NameConfig
// takes precedence over the OPENLINEAGE__NAME__ESCAPING environment variable.
// Pass a NameConfig with a nil Escaping field (or call ConfigureName(NameConfig{})
// again) to reset to env-var-only lookup.
func ConfigureName(cfg NameConfig) {
	nameConfigMu.Lock()
	defer nameConfigMu.Unlock()
	nameConfigGlobal = cfg.Escaping
}

// IsNameEscapingEnabled reports whether dot-escaping of name segments is
// enabled.
//
// Resolution order:
//
//  1. If [ConfigureName] was called with a non-nil Escaping value, that value
//     is returned.
//  2. Otherwise the environment variable OPENLINEAGE__NAME__ESCAPING is
//     consulted (case-insensitive; only "true" enables escaping).
func IsNameEscapingEnabled() bool {
	nameConfigMu.RLock()
	override := nameConfigGlobal
	nameConfigMu.RUnlock()

	if override != nil {
		return *override
	}
	return strings.EqualFold(strings.TrimSpace(os.Getenv(nameEscapingEnvVar)), "true")
}

// EscapeNameSegment escapes backslashes and dots in a single OpenLineage name
// segment.
//
// OpenLineage names are structured as dot-separated segments, e.g.
// "{database}.{schema}.{table}". When a segment itself contains a literal dot
// (e.g. an Oracle service name "mydb.example.com"), the dot must be escaped so
// that consumers can unambiguously split the name into its constituent parts.
//
// The transformation is applied in two steps so that backslashes already
// present in the segment are not misinterpreted as escape sequences:
//
//  1. A literal "\" is replaced with "\\".
//  2. A literal "." is replaced with "\.".
//
// This ensures that a segment such as "foo\.bar" (backslash followed by a dot)
// is encoded as "foo\\\\.bar", which a consumer can unambiguously decode.
//
// The transformation is applied only when [IsNameEscapingEnabled] returns true;
// otherwise the segment is returned unchanged.
func EscapeNameSegment(segment string) string {
	if !IsNameEscapingEnabled() {
		return segment
	}
	segment = strings.ReplaceAll(segment, "\\", "\\\\")
	return strings.ReplaceAll(segment, ".", "\\.")
}
