# OpenLineage Proxy [Helm Chart](https://helm.sh)

Helm Chart for [OpenLineage Proxy](https://github.com/OpenLineage/OpenLineage/tree/main/proxy).

## TL;DR

```bash
helm install ol-proxy . --dependency-update
```

## Installing the Chart

To install the chart with the release name `my-release`:

```bash
helm install ol-proxy .
```

> **Note:** For a list of parameters that can be overridden during installation, see the [configuration](#configuration) section.

## Uninstalling the Chart

To uninstall the `ol-proxy` deployment:

```bash
helm delete ol-proxy
```

## Configuration

### [Common](https://artifacthub.io/packages/helm/bitnami/common) **parameters**

| Parameter              | Description                         | Default |
|------------------------|-------------------------------------|---------|
| `commonLabels`         | Labels common to all resources      | `{}`    |
| `commonAnnotations`    | Annotations common to all resources | `{}`    |

### [OpenLineage Proxy](https://github.com/OpenLineage/OpenLineage/tree/main/proxy) **parameters**

| Parameter                        | Description                          | Default             |
|----------------------------------|--------------------------------------|---------------------|
| `proxy.replicaCount`             | Number of desired replicas           | `1`                 |
| `proxy.image.registry`           | Proxy image registry                 | `docker.io`         |
| `proxy.image.repository`         | Proxy image repository               | `openlineage/proxy` |
| `proxy.image.tag`                | Proxy image tag                      | `0.1.0`             |
| `proxy.image.pullPolicy`         | Image pull policy                    | `IfNotPresent`      |
| `proxy.host`                     | Proxy host                           | `localhost`         |
| `proxy.port`                     | API host port                        | `5000`              |
| `proxy.adminPort`                | Heath/Liveness host port             | `5001`              |
| `proxy.resources.limits`         | K8s resource limit overrides         | `nil`               |
| `proxy.resources.requests`       | K8s resource requests overrides      | `nil`               |
| `proxy.resources.limits`         | K8s resource limit overrides         | `nil`               |
| `proxy.resources.requests`       | K8s resource requests overrides      | `nil`               |
| `proxy.service.type`             | K8s service type                     | `ClusterIP`         |
| `proxy.service.port`             | K8s service port                     | `8080`              |
| `proxy.service.annotations`      | K8s service annotations              | `{}`                |
| `proxy.autoscaling.enabled`      | Enable autoscaling                   | `false`             |
| `proxy.autoscaling.minReplicas`  | Minimum number of replicas           | `1`                 |
| `proxy.autoscaling.maxReplicas`  | Maximum number of replicas           | `2`                 |
| `proxy.autoscaling.targetCPU`    | Target CPU utilization percentage    | `nil`               |
| `proxy.autoscaling.targetMemory` | Target Memory utilization percentage | `nil`               |
| `proxy.ingress.enabled`          | Enables ingress settings             | `false`             |
| `proxy.ingress.annotations`      | Annotations applied to ingress       | `nil`               |
| `proxy.ingress.hosts`            | Hostname applied to ingress routes   | `nil`               |
| `proxy.ingress.tls`              | TLS settings for hostname            | `nil`               |
| `proxy.podAnnotations`           | Additional pod annotations for Proxy | `{}`                |
