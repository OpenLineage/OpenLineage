/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package openlineage

import (
	"os"
	"strings"
)

const nameEscapingEnvVar = "OPENLINEAGE__NAME__ESCAPING"

// IsNameEscapingEnabled reports whether dot-escaping of name segments is
// enabled.
//
// Escaping is off by default and can be enabled by setting the environment
// variable OPENLINEAGE__NAME__ESCAPING=true (case-insensitive).
func IsNameEscapingEnabled() bool {
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
