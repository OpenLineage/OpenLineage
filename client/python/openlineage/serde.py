import json
from enum import Enum

import attr


class EventEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return obj.value
        if obj is None:
            return
        return super().default(obj)


class Serde:
    @staticmethod
    def to_json(obj):
        serialized = attr.asdict(obj)
        without_nulls = {k: v for k, v in serialized.items() if v is not None}
        return json.dumps(without_nulls, cls=EventEncoder, sort_keys=True)