# OpenLineage Temporal

The OpenLineage Temporal plugin leverages the OpenLineage Python client and Temporal Interceptors to emit start, complete, and failed events.

## Configuration

Register the plugin with a worker:

```py
client = await Client.connect("localhost:7233")
example_worker = Worker(
    client,
    task_queue="my-task-queue",
    workflows=[ExampleWorkflow],
    activities=[OpenLineagePlugin()],
)
```


