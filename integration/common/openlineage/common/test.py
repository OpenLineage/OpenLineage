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
import os
import logging
from typing import Any, Optional
from dateutil.parser import parse
from jinja2 import Environment


log = logging.getLogger(__name__)


def any(result: Any):
    return result


def is_datetime(result: Any):
    try:
        x = parse(result)  # noqa
        return "true"
    except:  # noqa
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


def setup_jinja() -> Environment:
    env = Environment()
    env.globals['any'] = any
    env.globals['is_datetime'] = is_datetime
    env.globals['env_var'] = env_var
    return env


env = setup_jinja()


def match(expected, result) -> bool:
    """
    Check if result is "equal" to expected value.
    """
    if isinstance(expected, dict):
        # Take a look only at keys present at expected dictionary
        for k, v in expected.items():
            if k not in result:
                log.error(f"Key {k} not in event {result}\nExpected {expected}")
                return False
            if not match(v, result[k]):
                log.error(f"For key {k}, value {v} not equals {result[k]}"
                          f"\nExpected {expected}, request {result}")
                return False
    elif isinstance(expected, list):
        if len(expected) != len(result):
            return False

        # Try to resolve case where we have wrongly sorted lists by looking at name attr
        # If name is not present then assume that lists are sorted
        for i, x in enumerate(expected):
            if 'name' in x:
                matched = False
                for y in result:
                    if 'name' in y and x['name'] == y['name']:
                        if not match(x, y):
                            return False
                        matched = True
                        break
                if not matched:
                    return False
            else:
                if not match(x, result[i]):
                    return False
    elif isinstance(expected, str):
        if expected.lstrip().startswith('{{'):
            # Evaluate jinja: in some cases, we want to check only if key exists, or if
            # value has the right type
            rendered = env.from_string(expected).render(result=result)
            if rendered == 'true' or rendered == result:
                return True
            log.error(f"Rendered value {rendered} does not equal 'true' or {result}")
            return False
        elif expected != result:
            return False
    elif expected != result:
        return False
    return True
