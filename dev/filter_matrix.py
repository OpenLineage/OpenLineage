# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import yaml

with open("complete_config.yml") as f:
    d = yaml.safe_load(f)

for _, workflow_definition in d["workflows"].items():
    jobs = workflow_definition.get("jobs") if isinstance(workflow_definition, dict) else None
    if not jobs:
        continue

    test_job = None
    integration_test_job = None

    for job in jobs:
        if "test-integration-spark" in job:
            test_job = job["test-integration-spark"]
        elif "integration-test-integration-spark" in job:
            integration_test_job = job["integration-test-integration-spark"]

    print("Test job: ", test_job)
    for job in [test_job, integration_test_job]:
        print("Filtering matrix for job: ", job.get("name"))
        print("Original matrix: ", job.get("matrix"))
        print("Original matrix parameters: ", job.get("matrix").get("parameters"))
        print(
            "Original matrix parameters env-variant: ", job.get("matrix").get("parameters").get("env-variant")
        )
        variants = [
            x for x in test_job.get("matrix").get("parameters").get("env-variant") if "full-tests" not in x
        ]
        job["matrix"]["parameters"]["env-variant"] = variants
with open("complete_config.yml", "w") as f:
    f.write(yaml.dump(d, sort_keys=False))
