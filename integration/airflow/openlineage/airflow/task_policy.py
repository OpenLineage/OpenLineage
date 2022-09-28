# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import TYPE_CHECKING, Callable, Union, Optional
from functools import wraps

from openlineage.airflow.adapter import OpenLineageAdapter
from openlineage.airflow.extractors import ExtractorManager
from openlineage.airflow.utils import (
    execute_in_thread,
    EventBuilder,
)

if TYPE_CHECKING:
    from airflow.models import BaseOperator, MappedOperator
    from airflow.models.abstractoperator import TaskStateChangeCallback


import logging

log = logging.getLogger(__name__)

extractor_manager = ExtractorManager()
adapter = OpenLineageAdapter()


def callback_wrapper(f):
    @wraps(f)
    def wrapper(callback: Optional["TaskStateChangeCallback"]):
        def func(context):
            task_instance = context["task_instance"]
            task = task_instance.task
            dag = context["dag"]
            dagrun = context["dag_run"]

            try:
                execute_in_thread(f, args=(context, task_instance, task, dag, dagrun))
            except Exception as e:
                log.error(f"Sending OpenLineage failed: {e}.")

            if callback:
                callback(context)

        return func

    return wrapper


@callback_wrapper
def on_execute(context, task_instance, task, dag, dagrun):
    run_id = EventBuilder.start_task(
        adapter=adapter,
        extractor_manager=extractor_manager,
        task_instance=task_instance,
        task=task,
        dag=dag,
        dagrun=dagrun,
    )
    context["_openlineage_run_id"] = run_id


@callback_wrapper
def on_success(context, task_instance, task, dag, dagrun):
    run_id = context["_openlineage_run_id"]
    EventBuilder.complete_task(
        adapter=adapter,
        extractor_manager=extractor_manager,
        task_instance=task_instance,
        task=task,
        dagrun=dagrun,
        run_id=run_id,
    )


@callback_wrapper
def on_failure(context, task_instance, task, dag, dagrun):
    run_id = context["_openlineage_run_id"]
    EventBuilder.fail_task(
        adapter=adapter,
        extractor_manager=extractor_manager,
        task_instance=task_instance,
        task=task,
        dagrun=dagrun,
        run_id=run_id,
    )


def _patch_policy(settings) -> None:
    if hasattr(settings, "task_policy"):
        task_policy = _wrap_task_policy(settings.task_policy)
        settings.task_policy = task_policy


def _wrap_task_policy(policy) -> Callable:
    if policy and hasattr(policy, "_task_policy_patched"):
        return policy

    def custom_task_policy(task: Union["BaseOperator", "MappedOperator"]) -> None:
        policy(task)
        task_policy(task)

    setattr(custom_task_policy, "_task_policy_patched", True)
    return custom_task_policy


def patch_openlineage_policy() -> None:
    try:
        import airflow_local_settings as settings
    except ImportError:
        from airflow import settings  # type: ignore
    _patch_policy(settings)


def task_policy(task: Union["BaseOperator", "MappedOperator"]) -> None:
    log.debug(f"Setting task policy for Dag: {task.dag_id} Task: {task.task_id}")
    if task.__class__.__name__ == "MappedOperator":
        _task_policy_mapped(task)  # type: ignore
    else:
        _task_policy_base(task)  # type: ignore


def _task_policy_mapped(task: "MappedOperator") -> None:
    task.partial_kwargs["on_execute_callback"] = on_execute(
        task.partial_kwargs["on_execute_callback"]
    )
    task.partial_kwargs["on_success_callback"] = on_success(
        task.partial_kwargs["on_success_callback"]
    )
    task.partial_kwargs["on_failure_callback"] = on_failure(
        task.partial_kwargs["on_failure_callback"]
    )


def _task_policy_base(task: "BaseOperator") -> None:
    task.on_execute_callback = on_execute(task.on_execute_callback)
    task.on_success_callback = on_success(task.on_success_callback)
    task.on_failure_callback = on_failure(task.on_failure_callback)
