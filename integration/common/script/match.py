# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
from openlineage.common.test import match


def check_matches(expected_events, actual_events) -> bool:
    for expected in expected_events:
        expected_job_name = expected['job']['name']
        is_compared = False
        for actual in actual_events:
            # Try to find matching event by eventType and job name
            if expected['eventType'] == actual['eventType'] \
                    and expected_job_name == actual['job']['name']:
                is_compared = True
                if not match(expected, actual):
                    print(f"failed to compare expected {expected}\nwith actual {actual}")
                    return False
                break
        if not is_compared:
            print(f"not found event comparable to {expected['eventType']} "
                  f"- {expected_job_name}")
            return False
    return True


def compare_mockserver(source_file, target_file):
    with open(source_file, 'r') as f:
        src = [obj['body'] for obj in json.load(f)]

    with open(target_file, 'r') as f:
        tgt = [obj['body'] for obj in json.load(f)]

    return check_matches(src, tgt)


if __name__ == "__main__":
    compare_mockserver("source.json", "target.json")
