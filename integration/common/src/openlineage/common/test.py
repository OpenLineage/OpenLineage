# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
import uuid
from typing import Any, Optional
from urllib.parse import urlparse

from dateutil.parser import parse
from jinja2 import Environment

log = logging.getLogger(__name__)


def any(result: Any):
    return result


def is_datetime(result: Any):
    try:
        parse(result)
        return "true"
    except Exception:
        pass
    return "false"


def is_uuid(result: Any):
    try:
        uuid.UUID(result)
        return "true"
    except Exception:
        pass
    return "false"


def env_var(var: str, default: Optional[str] = None) -> str:
    """The env_var() function. Return the environment variable named 'var'.
    If there is no such environment variable set, return the default.
    If the default is None, raise an exception for an undefined variable.
    """
    if var in os.environ:
        return os.environ[var]
    elif default is not None:
        return default
    else:
        msg = f"Env var required but not provided: '{var}'"
        raise Exception(msg)


def not_match(result, pattern) -> str:
    if pattern in result:
        raise Exception(f"Found {pattern} in {result}")
    return "true"


def url_scheme_authority(url) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def url_path(url) -> str:
    return urlparse(url).path


def setup_jinja() -> Environment:
    env = Environment()
    env.globals["any"] = any
    env.globals["is_datetime"] = is_datetime
    env.globals["is_uuid"] = is_uuid
    env.globals["env_var"] = env_var
    env.globals["not_match"] = not_match
    env.filters["url_scheme_authority"] = url_scheme_authority
    env.filters["url_path"] = url_path
    return env


env = setup_jinja()


def match(expected, result, ordered_list=False) -> bool:
    """
    Check if result is "equal" to expected value.
    """
    if isinstance(expected, dict):
        # Take a look only at keys present at expected dictionary
        for k, v in expected.items():
            if k not in result:
                log.error("Key %s not in received event %s\nExpected %s", k, result, expected)
                return False
            if not match(v, result[k], ordered_list):
                log.error(
                    "For key %s, expected value %s not equals received %s\nExpected %s, request %s",
                    k,
                    v,
                    result[k],
                    expected,
                    result,
                )
                return False
    elif isinstance(expected, list):
        if len(expected) != len(result):
            log.error("Length does not match: expected %s, got %s", len(expected), len(result))
            return False

        if ordered_list:
            for index, expected_elem, result_elem in zip(range(len(result)), expected, result):
                if not match(expected_elem, result_elem, ordered_list):
                    log.error(
                        "Elements in list don't match at index %s.\nExpected: %s\nGot: %s",
                        index,
                        expected_elem,
                        result_elem,
                    )
                    return False

            return True

        else:
            # Try to resolve case where we have wrongly sorted lists by looking at name attr
            # If name is not present then assume that lists are sorted

            for i, x in enumerate(expected):
                if "name" in x:
                    matched = False
                    for y in result:
                        if "name" in y:
                            expected_name = env.from_string(x["name"]).render()
                            result_name = env.from_string(y["name"]).render()
                            if expected_name == result_name:
                                if not match(x, y, ordered_list):
                                    log.error("List not matched %s where %s", x, y)
                                    return False
                        matched = True
                        break
                    if not matched:
                        log.error(
                            "List not matched - no same stuff for %s.\nExpected: %s\nGot: %s",
                            x,
                            expected,
                            result,
                        )
                        return False
                else:
                    if not match(x, result[i], ordered_list):
                        log.error(
                            "List not matched at %d.\nExpected: %s\nGot: %s",
                            i,
                            json.dumps(x),
                            json.dumps(result[i]),
                        )
                        return False
    elif isinstance(expected, str):
        if "{{" in expected:
            # Evaluate jinja: in some cases, we want to check only if key exists, or if
            # value has the right type
            rendered = env.from_string(expected).render(result=result)
            if rendered == "true" or rendered == result:
                return True
            log.error("Rendered value %s does not equal 'true' or %s", rendered, result)
            return False
        elif expected != result:
            log.error("Expected value %s does not equal result %s", expected, result)
            return False
    elif expected != result:
        log.error("Object of type %s: %s does not match %s", type(expected), expected, result)
        return False
    return True
