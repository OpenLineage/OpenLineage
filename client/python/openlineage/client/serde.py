# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import sys
from enum import Enum
from typing import Dict, List

import attr

log = logging.getLogger(__name__)

try:
    import numpy
except ImportError:
    log.warning("ImportError occurred when trying to import numpy module.")


class Serde:
    @classmethod
    def remove_nulls_and_enums(cls, obj):
        if isinstance(obj, Enum):
            return obj.value
        if isinstance(obj, Dict):
            return dict(filter(
                lambda x: x[1] is not None,
                {k: cls.remove_nulls_and_enums(v) for k, v in obj.items()}.items()
            ))
        if isinstance(obj, List):
            return list(filter(lambda x: x is not None and (isinstance(x, dict) and x != {}), [
                cls.remove_nulls_and_enums(v) for v in obj if v is not None
            ]))

        # Pandas can use numpy.int64 object
        if 'numpy' in sys.modules and isinstance(obj, numpy.int64):
            return int(obj)
        return obj

    @classmethod
    def to_dict(cls, obj):
        if not isinstance(obj, dict):
            obj = attr.asdict(obj)
        return cls.remove_nulls_and_enums(obj)

    @classmethod
    def to_json(cls, obj):
        return json.dumps(
            cls.to_dict(obj),
            sort_keys=True,
            default=lambda o: f"<<non-serializable: {type(o).__qualname__}>>"
        )
