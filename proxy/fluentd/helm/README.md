# OpenLineage-Fluentd [Helm Chart](https://helm.sh)

Helm Chart for demo of using [Fluentd](https://www.fluentd.org/) with [OpenLineage](https://github.com/OpenLineage/OpenLineage) plugin as proxy between client and [Marquez](https://github.com/MarquezProject/marquez).

## Prerequisites

- Kubernetes 1.12+
- Helm 3.1.0
- minikube

## Preparing environment

Start up minikube

```shell
minikube start
```

Make sure that minikube has ingress controllers with

```shell
minikube addons enable ingress
```

## Preparing OpenLineage

First thing we need to do is create docker image of Fluentd with Openlineage plugin and tag it.

```bash
docker build -t openlinaege/fluentd:1.16 -f ../docker/Dockerfile ../../..
```

## Installing the Chart

To install the chart with the release name `openlineage-fluentd-demo` using a fresh Postgres instance.

```bash
helm install openlineage-fluentd-demo . --dependency-update --set postgresql.enabled=true
```

> **Note:** For a list of parameters that can be overridden during installation, see the configuration section in Marquez chart readme.

## Uninstalling the Chart

To uninstall the `openlineage-fluentd-demo` deployment:

```bash
helm delete openlineage-fluentd-demo
```

## Testing

### Testing plugin
To test if the fluentd proxy is working execute

```shell
curl -X POST -d '{"test":"test"}' -H 'Content-Type: application/json' --resolve "openlineage-fluentd-demo:80:$( minikube ip )" -i http://openlineage-fluentd-demo/api/v1/lineage
```

this will send incorrect event to our proxy and should get a response 

```text
400 Bad Request
Openlineage validation failed: path "/": "run" is a required property, path "/": "job" is a required property, path "/": "eventTime" is a required property, path "/": "producer" is a required property, path "/": "schemaURL" is a required property
```

### Testing connection to marquez

To test if proxy connects to marquez check the state of marquez with

```shell
curl --resolve "openlineage-fluentd-demo-marquez:80:$( minikube ip )" -i http://openlineage-fluentd-demo/api/v1/namespaces/default/datasets
```

this should return 

```json
{"totalCount":0,"datasets":[]}
```
as there are no datasets in fresh marquez instance.

To populate marquez with some data execute

```shell
curl -X POST -d "$(cat test-event.json)" -H 'Content-Type: application/json' --resolve "openlineage-fluentd-demo:80:$( minikube ip )" -i http://openlineage-fluentd-demo/api/v1/lineage
```

then check again for datasets

```shell
curl --resolve "openlineage-fluentd-demo-marquez:80:$( minikube ip )" -i http://openlineage-fluentd-demo/api/v1/namespaces/default/datasets
```

now it should return

```json
{"totalCount":3,"datasets":[{"id":{"namespace":"default","name":"instance.schema.input-1"},"type":"DB_TABLE","name":"instance.schema.input-1","physicalName":"instance.schema.input-1","createdAt":"2020-12-28T09:52:00.001Z","updatedAt":"2020-12-28T09:52:00.001Z","namespace":"default","sourceName":"default","fields":[],"tags":[],"lastModifiedAt":null,"lastLifecycleState":"","description":null,"currentVersion":"e8f42e53-ee11-4c33-86be-427c6b4a6090","columnLineage":null,"facets":{},"deleted":false},{"id":{"namespace":"default","name":"instance.schema.input-2"},"type":"DB_TABLE","name":"instance.schema.input-2","physicalName":"instance.schema.input-2","createdAt":"2020-12-28T09:52:00.001Z","updatedAt":"2020-12-28T09:52:00.001Z","namespace":"default","sourceName":"default","fields":[],"tags":[],"lastModifiedAt":null,"lastLifecycleState":"","description":null,"currentVersion":"617ea54c-db2d-4d11-b746-5aa046711edc","columnLineage":null,"facets":{},"deleted":false},{"id":{"namespace":"default","name":"instance.schema.output-1"},"type":"DB_TABLE","name":"instance.schema.output-1","physicalName":"instance.schema.output-1","createdAt":"2020-12-28T09:52:00.001Z","updatedAt":"2023-06-15T11:59:41.458576Z","namespace":"default","sourceName":"default","fields":[],"tags":[],"lastModifiedAt":null,"lastLifecycleState":"","description":null,"currentVersion":"5bbfb88f-bfa9-4ade-a20a-29e400251e7a","columnLineage":null,"facets":{},"deleted":false}]}
```

Alternatively you can check the state of Marquez in UI, to do that you need execute

```shell
echo "$( minikube ip ) openlineage-fluentd-demo-marquez"
```

and append the result to `/etc/hosts`