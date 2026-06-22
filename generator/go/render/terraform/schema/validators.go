/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package schema

import (
	"github.com/hashicorp/terraform-plugin-framework-validators/objectvalidator"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
)

func requireFields(names ...string) []validator.Object {
	if len(names) == 0 {
		return nil
	}

	exprs := make([]path.Expression, 0, len(names))
	for _, n := range names {
		exprs = append(exprs, path.MatchRelative().AtName(n))
	}

	return []validator.Object{
		objectvalidator.AlsoRequires(exprs...),
	}
}
