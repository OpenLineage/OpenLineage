# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Dict, List, Optional


def get_from_nullable_chain(source: Any, chain: List[str]) -> Optional[Any]:
    """
    Get object from nested structure of objects, where it's not guaranteed that
    all keys in the nested structure exist.
    Intended to replace chain of `dict.get()` statements.
    Example usage:
    if not job._properties.get('statistics')\
        or not job._properties.get('statistics').get('query')\
        or not job._properties.get('statistics').get('query')\
            .get('referencedTables'):
        return None
    result = job._properties.get('statistics').get('query')\
            .get('referencedTables')
    becomes:
    result = get_from_nullable_chain(properties, ['statistics', 'query', 'queryPlan'])
    if not result:
        return None
    """
    chain.reverse()
    try:
        while chain:
            next_key = chain.pop()
            if isinstance(source, dict):
                source = source.get(next_key)
            else:
                source = getattr(source, next_key)
        return source
    except AttributeError:
        return None


def get_from_multiple_chains(source: Dict[str, Any], chains: List[List[str]]) -> Optional[Any]:
    for chain in chains:
        result = get_from_nullable_chain(source, chain)
        if result:
            return result
    return None


def parse_single_arg(args, keys: List[str], default=None) -> Optional[str]:
    """
    In provided argument list, find first key that has value and return that value.
    Values can be passed either as one argument {key}={value}, or two: {key} {value}
    """
    for key in keys:
        for i, arg in enumerate(args):
            if arg == key and len(args) > i:
                return args[i+1]
            if arg.startswith(f"{key}="):
                return arg.split("=", 1)[1]
    return default
