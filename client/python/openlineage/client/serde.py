# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
        dicted = attr.asdict(obj)
        return cls.remove_nulls_and_enums(dicted)

    @classmethod
    def to_json(cls, obj):
        return json.dumps(cls.to_dict(obj), sort_keys=True)
