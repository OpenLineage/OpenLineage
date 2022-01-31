# [`Kafka`](https://kafka.apache.org) Example

## 1. Start the Kafka broker and Proxy backend

```bash
docker-compose up -d
```

## 2. Emit an OpenLineage event to the Proxy backend

```bash
curl -X POST http://localhost:5000/api/v1/lineage \
  -H 'Content-Type: application/json' \
  -d '{
        "eventType": "START",
        "eventTime": "2020-12-28T19:52:00.001+10:00",
        "run": {
          "runId": "d46e465b-d358-4d32-83d4-df660ff614dd"
        },
        "job": {
          "namespace": "my-namespace",
          "name": "my-job"
        },
        "inputs": [{
          "namespace": "my-namespace",
          "name": "my-input"
        }],
        "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"
      }'
```

## 3. Read OpenLineage event from the `openlineage.events` topic

```bash
docker exec --interactive --tty broker \
kafka-console-consumer --bootstrap-server broker:9092 \
                         --topic openlineage.events \
                         --from-beginning
```

You’ll see the OpenLineage event that you emitted in **Step 2**:

```json
{
  "eventType": "START",
  "eventTime": "2020-12-28T19:52:00.001+10:00",
  "run": {
    "runId": "d46e465b-d358-4d32-83d4-df660ff614dd"
  },
  "job": {
    "namespace": "my-namespace",
    "name": "my-job"
  },
  "inputs": [{
    "namespace": "my-namespace",
    "name": "my-input"
  }],
  "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"
}
```