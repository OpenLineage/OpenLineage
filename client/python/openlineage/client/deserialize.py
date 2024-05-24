# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations


import re
import typing

import attrs
from cattrs import Converter
from cattrs.strategies import include_subclasses
from openlineage.client.event_v2 import BaseEvent
from openlineage.client.facet_v2 import BaseFacet
from openlineage.client.utils import keys_to_facets

converter: Converter | None = None

T = typing.TypeVar("T")


class ExtraFieldsSet:
    ...


def sanitize_field_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)


def choose_class(d: typing.Any, t: typing.Type[T]) -> dict[str, typing.Any]:
    def choose(key: str) -> typing.Type[T] | None:
        return keys_to_facets.get(key)

    def recreate_attrs_class(key: str) -> typing.Type[T]:
        data = d[key]
        if not isinstance(data, dict):
            return t
        _, typ = t.__args__  # type: ignore
        additional_fields = set(data.keys()) - set(attrs.fields_dict(typ).keys())
        if not additional_fields:
            return t
        additional_attrs = {
            sanitize_field_name(k): attrs.field(kw_only=True, metadata={"name": k}) for k in additional_fields
        }
        return attrs.make_class(
            typ.__name__,
            {
                **{
                    k: attrs.field(default=v.default, init=v.init, kw_only=v.kw_only, alias=v.alias)
                    for k, v in attrs.fields_dict(typ).items()
                },
                **additional_attrs,
            },
            bases=(ExtraFieldsSet, *typ.__mro__),
        )

    def derive_type(key: str) -> typing.Type[T]:
        typ = choose(key)
        if not typ:
            typ = recreate_attrs_class(key)
        return typ

    return {
        k: converter.structure({sanitize_field_name(k_): v_ for k_, v_ in v.items()}, derive_type(k))  # type: ignore
        for k, v in d.items()
    }


def is_dict_of_base_facet(cls: typing.Type[T]) -> bool:
    if typing.get_origin(cls) is typing.Union and type(None) in typing.get_args(cls):
        cls = typing.get_args(cls)[0]
    if type(cls) is not type and not issubclass(typing.get_origin(cls) or cls, dict):
        return False
    key_type, value_type = cls.__args__  # type: ignore
    return (
        key_type == str and issubclass(value_type, BaseFacet) and not issubclass(value_type, ExtraFieldsSet)
    )


def set_producer_and_schemaURL(d: typing.Any, t: typing.Type[BaseFacet]) -> BaseFacet:  #  noqa: N802
    producer = d.pop("_producer")
    schema_url = d.pop("_schemaURL")
    obj: BaseFacet = t(**d)
    if producer:
        obj._producer = producer
    if schema_url:
        obj._schemaURL = schema_url
    return obj


def get_converter() -> Converter:
    global converter  # noqa: PLW0603
    if not converter:
        converter = Converter()
        include_subclasses(BaseEvent, converter)
        converter.register_structure_hook_func(is_dict_of_base_facet, choose_class)
        converter.register_structure_hook_func(
            lambda x: issubclass(x, BaseFacet),
            set_producer_and_schemaURL,  # type: ignore
        )
    return converter


def load_event(d: dict[str, typing.Any]) -> BaseEvent:
    return get_converter().structure(d, BaseEvent)
