# Airflow task lifecycle

This document describes the airflow task lifecycle and describes how OpenLineage hooks into tasks to obtain information.

## Airflow lifecycle overview
https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#task-lifecycle

![](task_lifecycle_diagram.png)

The happy flow consists of the following stages:
1. No status (scheduler created empty task instance, a job and run with nominal start/end time is reported to OpenLineage)
2. Scheduled (scheduler determined task instance needs to run)
3. Queued (scheduler sent task to executor to run on the queue)
4. Running (worker picked up a task and is now running it)
5. Success/Skipped/Failed (task completed, OpenLineage is notified of the task's actual start/stop time with the status of the task instance)

## OpenLineage Interaction (Airflow 2.3+)
OpenLineage integration is an Airflow Plugin that provides listener instance.
Listeners are running on Airflow workers, and are invoked by Airflow when task starts and finishes, also when task fails.
[More information about listeners in Airflow docs.](https://github.com/apache/airflow/blob/main/docs/apache-airflow/listeners.rst)