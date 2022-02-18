# SPDX-License-Identifier: Apache-2.0.

import json
from enum import Enum
from typing import List, Dict

import attr

try:
    import numpy
except ImportError:
    numpy = None


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
            return list(filter(lambda x: x is not None and x != {}, [
                cls.remove_nulls_and_enums(v) for v in obj if v is not None
            ]))

        # Pandas can use numpy.int64 object
        if numpy and isinstance(obj, numpy.int64):
            return int(obj)
        return obj

    @classmethod
    def to_dict(cls, obj):
        if not isinstance(obj, dict):
            obj = attr.asdict(obj)
        return cls.remove_nulls_and_enums(obj)

    @classmethod
    def to_json(cls, obj):
        return json.dumps(cls.to_dict(obj), sort_keys=True,
                          default=lambda o: f"<<non-serializable: {type(o).__qualname__}>>")
