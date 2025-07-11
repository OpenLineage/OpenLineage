#!/usr/bin/env python
#
# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
import logging.config
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
    OPENLINEAGE_DBT_JOB_NAME_OPTION,
    PRODUCER,
    __version__,
    get_parent_run_metadata,
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
    run_id: Optional[str] = None,
    parent: Optional[ParentRunMetadata] = None,
) -> RunEvent:
    return RunEvent(
        eventType=state,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(
            runId=run_id or str(generate_new_uuid()),
            facets={"parent": parent.to_openlineage()} if parent else {},
        ),
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


def set_up_logger():
    """
    Set up the logger for the OpenLineage dbt wrapper.
    """
    log_format = "[%(asctime)s] [%(levelname)s] [%(name)s:%(lineno)d] - %(message)s"
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": log_format,
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "default",
                    "stream": sys.stdout,
                },
            },
            "loggers": {
                "openlineage": {
                    "handlers": ["console"],
                    "level": os.getenv("OPENLINEAGE_DBT_LOGGING", "INFO").upper(),
                    "propagate": True,
                },
            },
        }
    )
    logger = logging.getLogger("openlineage.dbt")
    custom_logging_level = os.getenv("OPENLINEAGE_CLIENT_LOGGING", None)
    if custom_logging_level:
        logger.setLevel(custom_logging_level)
    return logger


def main():
    logger = set_up_logger()

    logger.info("Running OpenLineage dbt wrapper version %s", __version__)

    args = sys.argv[1:]
    target = parse_single_arg(args, ["-t", "--target"])
    project_dir = parse_single_arg(args, ["--project-dir"], default="./")
    profile_name = parse_single_arg(args, ["--profile"])
    model_selector = parse_single_arg(args, ["--selector"])
    openlineage_job_name = parse_single_arg(args, [OPENLINEAGE_DBT_JOB_NAME_OPTION])
    models = parse_multiple_args(args, ["-m", "-s", "--model", "--models", "--select"])

    if openlineage_job_name:
        args = remove_command_line_option(args, OPENLINEAGE_DBT_JOB_NAME_OPTION, remove_value=True)
    else:
        openlineage_job_name = os.getenv("OPENLINEAGE_DBT_JOB_NAME")

    # dbt-ol option and not a dbt option
    consume_structured_logs_option = has_command_line_option(args, CONSUME_STRUCTURED_LOGS_COMMAND_OPTION)

    if consume_structured_logs_option:
        return consume_structured_logs(
            args=args,
            target=target,
            project_dir=project_dir,
            profile_name=profile_name,
            model_selector=model_selector,
            models=models,
            openlineage_job_name=openlineage_job_name,
        )
    else:
        return consume_local_artifacts(
            args=args,
            target=target,
            project_dir=project_dir,
            profile_name=profile_name,
            model_selector=model_selector,
            models=models,
            openlineage_job_name=openlineage_job_name,
        )


def consume_structured_logs(
    args: List[str],
    target: str,
    project_dir: str,
    profile_name: str,
    model_selector: str,
    models: List[str],
    openlineage_job_name: Optional[str] = None,
):
    logger = logging.getLogger("openlineage.dbt")
    logger.info(
        "This wrapper is using --consume-structured-logs: will send OpenLineage "
        "events while the models are executing."
    )
    job_namespace = os.environ.get("OPENLINEAGE_NAMESPACE", "dbt")
    dbt_command_line = remove_command_line_option(args, CONSUME_STRUCTURED_LOGS_COMMAND_OPTION)
    if not dbt_command_line or dbt_command_line[0] != "dbt":
        dbt_command_line = ["dbt"] + dbt_command_line
    processor = DbtStructuredLogsProcessor(
        project_dir=project_dir,
        dbt_command_line=dbt_command_line,
        producer=PRODUCER,
        target=target,
        job_namespace=job_namespace,
        openlineage_job_name=openlineage_job_name,
        profile_name=profile_name,
        logger=logger,
        models=models,
        selector=model_selector,
    )
    logger.info("dbt-ol will read logs from %s", processor.dbt_log_file_path)
    client = OpenLineageClient()
    emitted_events = 0
    try:
        for event in processor.parse():
            try:
                client.emit(event)
                emitted_events += 1
                if emitted_events % 50 == 0:
                    logger.debug("Processed %d events", emitted_events)
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
    except Exception:
        logger.exception(
            "OpenLineage failed to process dbt execution. This does not make dbt execution "
            "fail, however, data might not end up in your configured lineage backend."
        )
    finally:
        # Will wait for async events to be sent if async config is enabled
        logger.debug("Waiting for events to be sent.")
        client.close(timeout=30)

    logger.info("Emitted %d OpenLineage events", emitted_events)
    logger.info("Underlying dbt execution returned %d", processor.dbt_command_return_code)
    return processor.dbt_command_return_code


def consume_local_artifacts(
    args: List[str],
    target: str,
    project_dir: str,
    profile_name: str,
    model_selector: str,
    models: List[str],
    openlineage_job_name: Optional[str] = None,
):
    logger = logging.getLogger("openlineage.dbt")
    logger.info("This wrapper will send OpenLineage events at the end of dbt execution.")
    parent_id = os.getenv("OPENLINEAGE_PARENT_ID")
    parent_run_metadata = None
    # We can get this if we have been orchestrated by an external system like airflow
    job_namespace = os.environ.get("OPENLINEAGE_NAMESPACE", "dbt")

    if parent_id:
        parent_run_metadata = get_parent_run_metadata()
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
        openlineage_job_name=openlineage_job_name,
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
    # Set parent run metadata to use it as parent run facet
    processor.dbt_run_metadata = dbt_run_metadata

    pre_run_time = time.time()
    # Execute dbt in external process

    force_send_events = len(args) > 1 and args[1] == "send-events"
    if not force_send_events:
        with subprocess.Popen(["dbt"] + args, stdout=sys.stdout, stderr=sys.stderr) as process:
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
        terminal_event = dbt_run_event_end(
            run_id=dbt_run_metadata.run_id,
            job_namespace=dbt_run_metadata.job_namespace,
            job_name=dbt_run_metadata.job_name,
            parent_run_metadata=parent_run_metadata,
        )
    else:
        terminal_event = dbt_run_event_failed(
            run_id=dbt_run_metadata.run_id,
            job_namespace=dbt_run_metadata.job_namespace,
            job_name=dbt_run_metadata.job_name,
            parent_run_metadata=parent_run_metadata,
        )

    if events:
        # Pass some run facets from extracted dbt events to wrapping start and stop events
        event = events[0]
        if "dbt_version" in event.run.facets:
            start_event.run.facets["dbt_version"] = event.run.facets["dbt_version"]  # type: ignore[index]
            terminal_event.run.facets["dbt_version"] = event.run.facets["dbt_version"]  # type: ignore[index]

        if "processing_engine" in event.run.facets:
            start_event.run.facets["processing_engine"] = event.run.facets["processing_engine"]  # type: ignore[index]
            terminal_event.run.facets["processing_engine"] = event.run.facets["processing_engine"]  # type: ignore[index]

        if "dbt_run" in event.run.facets:
            start_event.run.facets["dbt_run"] = event.run.facets["dbt_run"]  # type: ignore[index]
            terminal_event.run.facets["dbt_run"] = event.run.facets["dbt_run"]  # type: ignore[index]

    emitted_events = 0
    all_events = [start_event, *events, terminal_event]
    for event in tqdm(
        all_events,
        desc="Emitting OpenLineage events",
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
    client.close(timeout=30)
    logger.info("Emitted %d OpenLineage events", emitted_events)
    logger.info("Underlying dbt execution returned %d", return_code)
    return return_code


if __name__ == "__main__":
    sys.exit(main())
