#!/usr/bin/env python
#
# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import List, Optional

from openlineage.client.client import OpenLineageClient
from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.facet_v2 import job_type_job
from openlineage.client.uuid import generate_new_uuid
from openlineage.common.provider.dbt import (
    DbtLocalArtifactProcessor,
    ParentRunMetadata,
    UnsupportedDbtCommand,
)
from openlineage.common.provider.dbt.structured_logs import DbtStructuredLogsProcessor
from openlineage.common.provider.dbt.utils import (
    CONSUME_STRUCTURED_LOGS_COMMAND_OPTION,
    PRODUCER,
    __version__,
)
from openlineage.common.utils import (
    has_command_line_option,
    parse_multiple_args,
    parse_single_arg,
    remove_command_line_option,
)
from tqdm import tqdm

JOB_TYPE_FACET = job_type_job.JobTypeJobFacet(
    jobType="JOB",
    integration="DBT",
    processingType="BATCH",
    producer=PRODUCER,
)


def dbt_run_event(
    state: RunState,
    job_name: str,
    job_namespace: str,
    run_id: str = str(generate_new_uuid()),
    parent: Optional[ParentRunMetadata] = None,
) -> RunEvent:
    return RunEvent(
        eventType=state,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=run_id, facets={"parent": parent.to_openlineage()} if parent else {}),
        job=Job(
            namespace=parent.job_namespace if parent else job_namespace,
            name=job_name,
            facets={"jobType": JOB_TYPE_FACET},
        ),
        producer=PRODUCER,
    )


def dbt_run_event_start(
    job_name: str, job_namespace: str, parent_run_metadata: ParentRunMetadata
) -> RunEvent:
    return dbt_run_event(
        state=RunState.START,
        job_name=job_name,
        job_namespace=job_namespace,
        parent=parent_run_metadata,
    )


def dbt_run_event_end(
    run_id: str,
    job_namespace: str,
    job_name: str,
    parent_run_metadata: Optional[ParentRunMetadata],
) -> RunEvent:
    return dbt_run_event(
        state=RunState.COMPLETE,
        job_namespace=job_namespace,
        job_name=job_name,
        run_id=run_id,
        parent=parent_run_metadata,
    )


def dbt_run_event_failed(
    run_id: str,
    job_namespace: str,
    job_name: str,
    parent_run_metadata: Optional[ParentRunMetadata],
) -> RunEvent:
    return dbt_run_event(
        state=RunState.FAIL,
        job_namespace=job_namespace,
        job_name=job_name,
        run_id=run_id,
        parent=parent_run_metadata,
    )


openlineage_logger = logging.getLogger("openlineage.dbt")
openlineage_logger.setLevel(os.getenv("OPENLINEAGE_DBT_LOGGING", "INFO"))
openlineage_logger.addHandler(logging.StreamHandler(sys.stdout))
# deprecated dbtol logger
logger = logging.getLogger("dbtol")
for handler in openlineage_logger.handlers:
    logger.addHandler(handler)
    logger.setLevel(openlineage_logger.level)


def main():
    logger.info("Running OpenLineage dbt wrapper version %s", __version__)

    args = sys.argv[1:]
    target = parse_single_arg(args, ["-t", "--target"])
    project_dir = parse_single_arg(args, ["--project-dir"], default="./")
    profile_name = parse_single_arg(args, ["--profile"])
    model_selector = parse_single_arg(args, ["--selector"])
    models = parse_multiple_args(args, ["-m", "-s", "--model", "--models", "--select"])

    # dbt-ol option and not a dbt option
    consume_structured_logs_option = has_command_line_option(args, CONSUME_STRUCTURED_LOGS_COMMAND_OPTION)

    if consume_structured_logs_option:
        return consume_structured_logs(
            target=target,
            project_dir=project_dir,
            profile_name=profile_name,
            model_selector=model_selector,
            models=models,
        )
    else:
        return consume_local_artifacts(
            target=target,
            project_dir=project_dir,
            profile_name=profile_name,
            model_selector=model_selector,
            models=models,
        )


def consume_structured_logs(
    target: str, project_dir: str, profile_name: str, model_selector: str, models: List[str]
):
    logger.info("This wrapper will send OpenLineage events while the models are executing.")
    return_code = 0
    job_namespace = os.environ.get("OPENLINEAGE_NAMESPACE", "dbt")
    dbt_command_line = remove_command_line_option(sys.argv, CONSUME_STRUCTURED_LOGS_COMMAND_OPTION)
    dbt_command_line = ["dbt"] + dbt_command_line[1:]
    processor = DbtStructuredLogsProcessor(
        project_dir=project_dir,
        dbt_command_line=dbt_command_line,
        producer=PRODUCER,
        target=target,
        job_namespace=job_namespace,
        profile_name=profile_name,
        logger=logger,
        models=models,
        selector=model_selector,
    )

    client = OpenLineageClient()
    last_event = None
    emitted_events = 0
    try:
        for event in processor.parse():
            try:
                last_event = event
                client.emit(event)
                emitted_events += 1
            except Exception as e:
                logger.warning(
                    "OpenLineage client failed to emit event %s runId %s. Exception: %s",
                    event.eventType.value,
                    event.run.runId,
                    e,
                    exc_info=True,
                )
    except UnsupportedDbtCommand as e:
        logger.error(e)
        return_code = 1

    if last_event and last_event.eventType != RunState.COMPLETE:
        return_code = 1

    logger.info("Emitted %d OpenLineage events", emitted_events)
    return return_code


def consume_local_artifacts(
    target: str, project_dir: str, profile_name: str, model_selector: str, models: List[str]
):
    logger.info("This wrapper will send OpenLineage events at the end of dbt execution.")
    parent_id = os.getenv("OPENLINEAGE_PARENT_ID")
    parent_run_metadata = None
    # We can get this if we have been orchestrated by an external system like airflow
    job_namespace = os.environ.get("OPENLINEAGE_NAMESPACE", "dbt")

    if parent_id:
        parent_namespace, parent_job_name, parent_run_id = parent_id.split("/")
        parent_run_metadata = ParentRunMetadata(
            run_id=parent_run_id,
            job_name=parent_job_name,
            job_namespace=parent_namespace,
        )

    client = OpenLineageClient()

    processor = DbtLocalArtifactProcessor(
        producer=PRODUCER,
        target=target,
        job_namespace=job_namespace,
        project_dir=project_dir,
        profile_name=profile_name,
        logger=logger,
        models=models,
        selector=model_selector,
    )

    # Always emit "wrapping event" around dbt run. This indicates start of dbt execution, since
    # both the start and complete events for dbt models won't get emitted until end of execution.
    start_event = dbt_run_event_start(
        job_name=processor.job_name,
        job_namespace=job_namespace,
        parent_run_metadata=parent_run_metadata,
    )

    dbt_run_metadata = ParentRunMetadata(
        run_id=start_event.run.runId,
        job_name=start_event.job.name,
        job_namespace=start_event.job.namespace,
    )

    # Failed start event emit should not stop dbt command from running.
    emitted_events = 0
    try:
        client.emit(start_event)
        emitted_events += 1
    except Exception as e:
        logger.warning("OpenLineage client failed to emit start event. Exception: %s", e)

    # Set parent run metadata to use it as parent run facet
    processor.dbt_run_metadata = dbt_run_metadata

    pre_run_time = time.time()
    # Execute dbt in external process

    force_send_events = len(sys.argv) > 1 and sys.argv[1] == "send-events"
    if not force_send_events:
        with subprocess.Popen(["dbt"] + sys.argv[1:], stdout=sys.stdout, stderr=sys.stderr) as process:
            return_code = process.wait()
    else:
        logger.warning("Sending events for the last run without running the job")
        return_code = 0

    # If run_result has modification time before dbt command
    # or does not exist, do not emit dbt events.
    try:
        if os.stat(processor.run_result_path).st_mtime < pre_run_time and not force_send_events:
            logger.info(
                "OpenLineage events not emitted: run_result file (%s) was not modified by dbt",
                processor.run_result_path,
            )
            return return_code
    except FileNotFoundError:
        logger.info(
            "OpenLineage events not emitted: did not find run_result file (%s)", processor.run_result_path
        )
        return return_code

    try:
        events = processor.parse().events()
    except UnsupportedDbtCommand as e:
        # log exception message
        logger.info(e)
        events = []

    if return_code == 0:
        last_event = dbt_run_event_end(
            run_id=dbt_run_metadata.run_id,
            job_namespace=dbt_run_metadata.job_namespace,
            job_name=dbt_run_metadata.job_name,
            parent_run_metadata=parent_run_metadata,
        )
    else:
        last_event = dbt_run_event_failed(
            run_id=dbt_run_metadata.run_id,
            job_namespace=dbt_run_metadata.job_namespace,
            job_name=dbt_run_metadata.job_name,
            parent_run_metadata=parent_run_metadata,
        )

    for event in tqdm(
        events + [last_event],
        desc="Emitting OpenLineage events",
        initial=1,
        total=len(events) + 2,
    ):
        try:
            client.emit(event)
            emitted_events += 1
        except Exception as e:
            logger.warning(
                "OpenLineage client failed to emit event %s runId %s. Exception: %s",
                event.eventType.value,
                event.run.runId,
                e,
                exc_info=True,
            )
    logger.info("Emitted %d OpenLineage events", emitted_events)
    return return_code


if __name__ == "__main__":
    sys.exit(main())
