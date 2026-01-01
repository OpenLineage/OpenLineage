# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import yaml

with open("complete_config.yml") as f:
    d = yaml.safe_load(f)

for _, workflow_definition in d["workflows"].items():
    jobs = workflow_definition.get("jobs") if isinstance(workflow_definition, dict) else None
    if not jobs:
        continue

    # find all approvals
    approvals = list(
        filter(lambda x: isinstance(x, dict) and next(iter(x.values())).get("type") == "approval", jobs)
    )
    for approval in approvals:
        approval_name = next(iter(approval))
        approval_upstreams = approval[approval_name].get("requires")
        approval_downstream = list(
            filter(
                lambda x: isinstance(x, dict) and approval_name in next(iter(x.values())).get("requires", ""),
                jobs,
            )
        )
        # replace approval with its upstream jobs
        for job in approval_downstream:
            requires = next(iter(job.values()))["requires"]
            requires.remove(approval_name)
            requires.extend(approval_upstreams)
        jobs.remove(approval)
with open("complete_config.yml", "w") as f:
    f.write(yaml.dump(d, sort_keys=False))
