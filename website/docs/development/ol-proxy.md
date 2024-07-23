---
title: OpenLineage Proxy
sidebar_position: 3
---

OpenLineage Proxy is a simple Java server that can be used to monitor the JSON events that OpenLineage client emits, as well as tunnel the transmission to the OpenLineage backend such as [Marquez](https://marquezproject.ai/).

When you are unable to collect logs on the client side, but want to make sure the event that gets emitted are valid and correct, you can use OpenLineage Proxy to verify the messages.

## Accessing the proxy
OpenLineage proxy can be obtained via github:
```
git clone https://github.com/OpenLineage/OpenLineage.git
cd OpenLineage/proxy/backend
```

## Building the proxy
To build the proxy jar, run
```
$ ./gradlew build
```

The packaged jar file can be found under `./build/libs/`

## Running the proxy

OpenLineage Proxy requires configuration file named `proxy.yml`. There is an [example](https://github.com/OpenLineage/OpenLineage/blob/main/proxy/backend/proxy.example.yml) that you can copy and name it as `proxy.yml`.

```
cp proxy.example.yml proxy.yml
```

By default, the OpenLineage proxy uses the following ports:

- TCP port 8080 is available for the HTTP API server.
- TCP port 8081 is available for the admin interface.

You can then run the proxy using gradlew:
```
$ ./gradlew runShadow
```

## Monitoring OpenLineage events via Proxy

When proxy is running, you can start sending your OpenLineage events just as the same way as you would be sending to any OpenLineage backend server. For example, in your URL for the OpenLineage backend, you can specify it as `http://localhost:8080/api/v1/lineage`.

Once the message is sent to the proxy, you will see the OpenLineage message content (JSON) to the console output of the proxy. You can also specify in the configuration to store the messages into the log file.

> You might have noticed that OpenLineage client (python, java) simply requires `http://localhost:8080` as the URL endpoint. This is possible because the client code adds the `/api/v1/lineage` internally before it makes the request. If you are not using OpenLineage client library to emit OpenLineage events, you must use the full URL in order for the proxy to receive the data correctly.

## Forwarding the data
Not only the OpenLineage proxy is useful in receiving the monitoring the OpenLineage events, it can also be used to relay the events to other endpoints. Please see the [example](https://github.com/OpenLineage/OpenLineage/blob/main/proxy/backend/proxy.example.yml) of how to set the proxy to relay the events via Kafka topic or HTTP endpoint.

## Other ways to run OpenLineage Proxy
- You do not have to clone the git repo and build all the time. OpenLineage proxy is published and available in [Maven Repository](https://mvnrepository.com/artifact/io.openlineage/openlineage-proxy/).
- You can also run OpenLineage Proxy as a [docker container](https://github.com/OpenLineage/OpenLineage/blob/main/proxy/backend/Dockerfile).
- There is also a [helm chart for Kubernetes](https://github.com/OpenLineage/OpenLineage/tree/main/proxy/backend/chart) available.
