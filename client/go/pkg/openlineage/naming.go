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

// EscapeNameSegment escapes dots in a single OpenLineage name segment.
//
// OpenLineage names are structured as dot-separated segments, e.g.
// "{database}.{schema}.{table}". When a segment itself contains a literal dot
// (e.g. an Oracle service name "mydb.example.com"), the dot must be escaped so
// that consumers can unambiguously split the name into its constituent parts.
//
// The escaping rule (from the naming specification) is: a literal "." inside a
// segment is written as "\\.".
//
// The transformation is applied only when [IsNameEscapingEnabled] returns true;
// otherwise the segment is returned unchanged.
func EscapeNameSegment(segment string) string {
	if !IsNameEscapingEnabled() {
		return segment
	}
	return strings.ReplaceAll(segment, ".", "\\.")
}
