# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import importlib
import logging
import sys
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


# Filter dictionary to get only those key: value pairs that have
# key specified in passed attr class
def get_only_specified_fields(clazz: type[Any], params: dict[str, Any]) -> dict[str, Any]:
    field_keys = [item.name for item in attr.fields(clazz)]
    return {key: value for key, value in params.items() if key in field_keys}


def deep_merge_dicts(dict1: dict[Any, Any], dict2: dict[Any, Any]) -> dict[Any, Any]:
    """Deep merges two dictionaries.

    This function merges two dictionaries while handling nested dictionaries.
    For keys that exist in both dictionaries, the values from dict2 take precedence.
    If a key exists in both dictionaries and the values are dictionaries themselves,
    they are merged recursively.
    This function merges only dictionaries. If key is of different type, e.g. list
    it does not work properly.
    """
    merged = dict1.copy()
    for k, v in dict2.items():
        if k in merged and isinstance(v, dict):
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


class RedactMixin:
    _skip_redact: ClassVar[list[str]] = []

    @property
    def skip_redact(self) -> list[str]:
        return self._skip_redact
