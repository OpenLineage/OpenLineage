# Airflow task lifecycle

This document describes the Airflow task lifecycle and describes how OpenLineage hooks into tasks to obtain information.

## Airflow lifecycle overview
https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#task-lifecycle

![](task_lifecycle_diagram.png)

A happy flow consists of the following stages:
1. No status (the scheduler created an empty task instance, and a job and run with nominal start/end time is reported to OpenLineage)
2. Scheduled (the scheduler determined a task instance needs to run)
3. Queued (the scheduler sent a task to the executor to run on the queue)
4. Running (a worker picked up a task and is now running it)
5. Success/Skipped/Failed (the task completed, and OpenLineage is notified of the task's actual start/stop time with the status of the task instance)

## OpenLineage Interaction (Airflow 2.3+)
The OpenLineage integration is an Airflow Plugin that provides a listener instance.
Listeners are running on Airflow workers and are invoked by Airflow when a task starts and finishes, as well as when a task fails.
[More information about listeners in Airflow docs.](https://github.com/apache/airflow/blob/main/docs/apache-airflow/listeners.rst)