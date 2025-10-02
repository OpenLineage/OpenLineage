# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import logging
from enum import Enum
from typing import Any, cast

import attr

log = logging.getLogger(__name__)


class Serde:
    @classmethod
    def remove_nulls_and_enums(cls, obj: Any) -> Any:
        if isinstance(obj, Enum):
            return obj.value
        if isinstance(obj, dict):
            return dict(
                filter(
                    lambda x: x[1] is not None,
                    {k: cls.remove_nulls_and_enums(v) for k, v in obj.items()}.items(),
                ),
            )
        if isinstance(obj, list):
            return list(
                filter(
                    lambda x: x is not None and (not isinstance(x, dict) or x != {}),
                    [cls.remove_nulls_and_enums(v) for v in obj if v is not None],
                ),
            )

        # Pandas can use numpy.int64 object
        if getattr(getattr(obj, "__class__", None), "__name__", None) == "int64":
            return int(obj)
        return obj

    @classmethod
    def to_dict(cls, obj: Any) -> dict[Any, Any]:
        if not isinstance(obj, dict):
            obj = attr.asdict(obj)
        return cast("dict[Any, Any]", cls.remove_nulls_and_enums(obj))

    @classmethod
    def to_json(cls, obj: Any) -> str:
        return json.dumps(
            cls.to_dict(obj),
            sort_keys=True,
            default=lambda o: f"<<non-serializable: {type(o).__qualname__}>>",
        )
