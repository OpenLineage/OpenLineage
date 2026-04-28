/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

// Package genutil contains shared utilities for the facet-gen code generator.
package genutil

import (
	"strings"
	"unicode"
)

// ToSnake converts a camelCase or PascalCase string to snake_case.
func ToSnake(s string) string {
	var out []rune
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 {
				out = append(out, '_')
			}
			out = append(out, unicode.ToLower(r))
		} else {
			out = append(out, r)
		}
	}
	return string(out)
}

// FormatGoFieldName applies OpenLineage acronym-capitalization rules to a PascalCase
// Go field name: URI, URL, and Id suffixes are promoted to their canonical Go
// initialisations (URI → URI, URL → URL, Id → ID) per the Go naming conventions
// documented in https://github.com/golang/go/wiki/CodeReviewComments#initialisms.
func FormatGoFieldName(name string) string {
	if strings.HasSuffix(name, "Uri") {
		return strings.TrimSuffix(name, "Uri") + "URI"
	}
	if strings.HasSuffix(name, "Url") {
		return strings.TrimSuffix(name, "Url") + "URL"
	}
	if strings.HasSuffix(name, "Id") {
		return strings.TrimSuffix(name, "Id") + "ID"
	}
	return name
}

// LowerFirst lowercases the first rune of s.
func LowerFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}

// UpperFirst lowercases the first rune of s.
func UpperFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}
