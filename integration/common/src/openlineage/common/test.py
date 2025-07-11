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
                log.error(f"Key {k} not in received event {result}\nExpected {expected}")
                return False
            if not match(v, result[k], ordered_list):
                log.error(
                    f"For key {k}, expected value {v} not equals received {result[k]}"
                    f"\nExpected {expected}, request {result}"
                )
                return False
    elif isinstance(expected, list):
        if len(expected) != len(result):
            log.error(f"Length does not match: expected {len(expected)} result: {len(result)}")
            return False

        if ordered_list:
            for index, expected_elem, result_elem in zip(range(len(result)), expected, result):
                if not match(expected_elem, result_elem, ordered_list):
                    log.error(
                        f"Elements in list don't match at index {index} - "
                        f"expected element={expected_elem} VS actual result = {result_elem}"
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
                                    log.error(f"List not matched {x} where {y}")
                                    return False
                        matched = True
                        break
                    if not matched:
                        log.error(
                            f"List not matched - no same stuff for {x} "
                            f"in expected: {expected} result {result}"
                        )
                        return False
                else:
                    if not match(x, result[i], ordered_list):
                        log.error(
                            f"List not matched at {i}\n"
                            + f"  expected:\n{json.dumps(x)}\n"
                            + f"  result: \n{json.dumps(result[i])}"
                        )
                        return False
    elif isinstance(expected, str):
        if "{{" in expected:
            # Evaluate jinja: in some cases, we want to check only if key exists, or if
            # value has the right type
            rendered = env.from_string(expected).render(result=result)
            if rendered == "true" or rendered == result:
                return True
            log.error(f"Rendered value {rendered} does not equal 'true' or {result}")
            return False
        elif expected != result:
            log.error(f"Expected value {expected} does not equal result {result}")
            return False
    elif expected != result:
        log.error(f"Object of type {type(expected)}: {expected} does not match {result}")
        return False
    return True
