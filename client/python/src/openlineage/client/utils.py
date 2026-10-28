# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import importlib
import json
import logging
import sys
from collections import OrderedDict
from typing import Any, ClassVar, cast

import attr

log = logging.getLogger(__name__)

# Classes created by `add_additional_properties` via `attr.make_class`, keyed by the class they
# extend and the set of extra field names added. Reusing the same class object for a given
# (base class, extra fields) pair keeps repeated calls cheap and, more importantly, lets
# pickled/unpickled instances (see `reduce_with_additional_properties`) compare equal to one
# another.
_DYNAMIC_ADDITIONAL_PROPERTIES_CLASSES: dict[tuple[type, frozenset[str]], type] = {}


def import_from_string(path: str) -> type[Any]:
    try:
        module_path, target = path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return cast("type[Any]", getattr(module, target))
    except Exception as e:
        log.warning(e)
        msg = f"Failed to import {path}"
        raise ImportError(msg) from e


def try_import_from_string(path: str) -> type[Any] | None:
    try:
        return import_from_string(path)
    except ImportError:
        return None


class EnvFloat(float):
    """A float decoded from an environment variable whose JSON text does not round-trip.

    `0.50`, `1e5` or `1234e5678` (which decodes to `inf`) decode to a float that does not print
    back as written. Numeric settings use the value as a regular float, while
    :func:`config_value_to_str` returns `text`, so string settings such as tag values or API keys
    keep what was written.
    """

    text: str

    def __new__(cls, value: float, text: str) -> EnvFloat:
        number = super().__new__(cls, value)
        number.text = text
        return number

    def __getnewargs__(self) -> tuple[float, str]:  # type: ignore[override]
        return float(self), self.text


class EnvInt(int):
    """An int decoded from an environment variable whose JSON text does not round-trip, like `-0`.

    See :class:`EnvFloat`.
    """

    text: str

    def __new__(cls, value: int, text: str) -> EnvInt:
        number = super().__new__(cls, value)
        number.text = text
        return number

    def __getnewargs__(self) -> tuple[int, str]:  # type: ignore[override]
        return int(self), self.text


def config_value_to_str(value: Any) -> str:
    """Return the string form of a scalar value given for a string-typed config setting.

    Environment variable values are JSON-decoded and YAML scalars are typed, so a string
    setting written as ``true``, ``42`` or ``1.0`` arrives as ``bool``, ``int`` or ``float``.
    Booleans become ``"true"``/``"false"`` and numbers their JSON text, which matches what
    was written in the environment variable, the YAML file, and what the Java client emits.
    Numbers decoded from environment variables whose JSON text differs from what was written
    (:class:`EnvFloat`, :class:`EnvInt`) return the text as written.

    Raises:
        TypeError: for ``None`` and non-scalar values (dicts, lists, objects); callers decide
            whether to skip the value or fail.
    """
    if isinstance(value, str):
        return value
    if isinstance(value, (EnvFloat, EnvInt)):
        return value.text
    if isinstance(value, (bool, int, float)):
        return json.dumps(value)
    msg = f"Expected a string, boolean or number, got {type(value).__name__}: {value!r}"
    raise TypeError(msg)


def optional_config_value_to_str(value: Any) -> str | None:
    """Like :func:`config_value_to_str`, but passes ``None`` through."""
    return None if value is None else config_value_to_str(value)


def config_values_to_str(values: dict[Any, Any] | None) -> dict[Any, Any]:
    """Turn boolean and numeric values of a string-to-string mapping into strings.

    Uses :func:`config_value_to_str`; values it does not accept (``None``, bytes, ...) are kept
    as given.
    """

    def to_str_if_scalar(value: Any) -> Any:
        try:
            return config_value_to_str(value)
        except TypeError:
            return value

    return {k: to_str_if_scalar(v) for k, v in (values or {}).items()}


def first_config_value(config: dict[str, Any], *keys: str) -> Any:
    """Return the value of the first of `keys` that is set to something other than ``None`` or ``""``.

    Unlike chaining ``config.get(a) or config.get(b)``, this keeps ``0`` and ``False``, which is
    what a string setting written as ``0`` or ``false`` decodes to.
    """
    for key in keys:
        value = config.get(key)
        if value is not None and value != "":
            return value
    return None


# Filter dictionary to get only those key: value pairs that have
# key specified in passed attr class
def get_only_specified_fields(clazz: type[Any], params: dict[str, Any]) -> dict[str, Any]:
    field_keys = [item.name for item in attr.fields(clazz)]
    return {key: value for key, value in params.items() if key in field_keys}


def deep_merge_dicts(dict1: dict[Any, Any], dict2: dict[Any, Any]) -> dict[Any, Any]:
    """Deep merges two dictionaries.

    This function merges two dictionaries while handling nested dictionaries.
    For keys that exist in both dictionaries, the values from dict2 take precedence.
    Two values are merged recursively only when both of them are dictionaries;
    otherwise the value from dict2 replaces the one from dict1.
    """
    merged = dict1.copy()
    for k, v in dict2.items():
        if k in merged and isinstance(v, dict) and isinstance(merged.get(k), dict):
            merged[k] = deep_merge_dicts(merged.get(k, {}), v)
        else:
            merged[k] = v
    return merged


def _is_importable_class(cls: type) -> bool:
    """Return whether `cls` can be found by pickle via its `__module__`/`__qualname__`."""
    return getattr(sys.modules.get(cls.__module__), cls.__qualname__, None) is cls


def add_additional_properties(instance: Any, **kwargs: Any) -> Any:
    """Return a copy of ``instance`` with extra attributes added.

    Backs the generated ``with_additional_properties`` methods. Builds (and caches, per class
    and set of extra field names) a new ``attrs`` subclass at runtime via `attr.make_class` to
    hold any keyword that isn't an existing field.
    """
    current_attrs = [a.name for a in attr.fields(instance.__class__)]
    new_attr_names = frozenset(k for k in kwargs if k not in current_attrs)

    cache_key = (instance.__class__, new_attr_names)
    new_class = _DYNAMIC_ADDITIONAL_PROPERTIES_CLASSES.get(cache_key)
    if new_class is None:
        new_class = attr.make_class(
            instance.__class__.__name__,
            {k: attr.field(default=None) for k in new_attr_names},
            bases=(instance.__class__,),
        )
        new_class.__module__ = instance.__class__.__module__
        _DYNAMIC_ADDITIONAL_PROPERTIES_CLASSES[cache_key] = new_class

    for a in attr.fields(instance.__class__):
        if not a.init:
            continue
        attr_name = a.name  # To deal with private attributes.
        init_name = a.alias
        if init_name not in kwargs:
            kwargs[init_name] = getattr(instance, attr_name)
    return new_class(**kwargs)


def reduce_with_additional_properties(instance: Any) -> tuple[Any, ...]:
    """Generic ``__reduce__`` for classes generated with a ``with_additional_properties`` method.

    That method builds a new class at runtime via `attr.make_class`. The new class shares its
    name with the original, importable class but is never bound to it in the module namespace,
    so pickle's default class lookup resolves to the original class instead and raises a
    ``PicklingError``. When that's the case, reduce to a call that rebuilds the dynamic class
    instead of referencing it directly.
    """
    cls = instance.__class__
    if _is_importable_class(cls):
        return cast("tuple[Any, ...]", object.__reduce__(instance))

    base_cls = next(c for c in cls.__mro__[1:] if _is_importable_class(c))

    # Walk back from `cls` to `base_cls`, recording the field names each dynamic class added
    # on top of its parent, so they can be replayed in the same order on unpickling.
    levels: list[frozenset[str]] = []
    c = cls
    while c is not base_cls:
        parent = c.__bases__[0]
        own_names = frozenset(a.name for a in attr.fields(c))
        own_names -= frozenset(a.name for a in attr.fields(parent))
        levels.append(own_names)
        c = parent
    levels.reverse()

    kwargs = {a.alias: getattr(instance, a.name) for a in attr.fields(cls) if a.init}
    return (_rebuild_with_additional_properties, (base_cls, kwargs, tuple(levels)))


def _rebuild_with_additional_properties(
    base_cls: type, kwargs: dict[str, Any], levels: tuple[frozenset[str], ...]
) -> Any:
    """Reconstruct an instance built by ``with_additional_properties``, used when unpickling."""
    base_field_aliases = {a.alias for a in attr.fields(base_cls) if a.init}
    instance = base_cls(**{k: v for k, v in kwargs.items() if k in base_field_aliases})
    for level_names in levels:
        instance = instance.with_additional_properties(**{k: kwargs[k] for k in level_names})
    return instance


def split_into_list(value: str | list[str]) -> list[str]:
    if isinstance(value, list):
        return value
    return [item.strip() for item in value.split(";") if item.strip()]


class RedactMixin:
    _skip_redact: ClassVar[list[str]] = []

    @property
    def skip_redact(self) -> list[str]:
        return self._skip_redact


class LogDeduplicationFilter(logging.Filter):
    """
    Deduplicates log records with level > DEBUG based on the fully formatted
    message. First occurrence is logged at the original level; subsequent
    occurrences are demoted to DEBUG.

    Deduplication scope is bounded using an LRU cache to prevent unbounded
    memory growth.
    """

    def __init__(self, max_messages: int = 5_000) -> None:
        super().__init__()
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._max = max_messages

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno <= logging.DEBUG:
            return True

        message = record.getMessage()

        if message in self._seen:
            record.levelno = logging.DEBUG
            record.levelname = logging.getLevelName(logging.DEBUG)
            self._seen.move_to_end(message)
        else:
            self._seen[message] = None
            if len(self._seen) > self._max:
                self._seen.popitem(last=False)

        return True
