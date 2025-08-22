# OpenLineage Hive Docker Image Builder

This project is responsible for building and publishing Docker images for different versions of
Hive to `quay.io/openlineage/hive`. These images are used in the integration tests of the
OpenLineage Hive integration.

## Steps

For building images locally:

1. Create a builder for the Docker images using the following
   command: `docker buildx create --use --name openlineage-hive-builder --driver docker-container`
2. Run the Gradle task to build the Docker images: `./gradlew buildAllDockerImages -PskipPush=true`

For pushing images:

1. Create a builder for the Docker images using the following
2. Login to `quay.io` using the following command: `docker login quay.io`
3. Run the Gradle task to build and push the Docker images: `./gradlew buildAllDockerImages`

## manifest.json

The `manifest.json` file is used to specify the different versions of Hive for which Docker images
should be built. Each object in the JSON array represents a different Hive version and contains the
following properties:

- `hadoopVersion`: Version of hadoop libraries to use.
- `hiveVersion`: Version of hive libraries to use.
- `tezVersion`: Version of tez libraries to use.

How can get the appropriate versions
1. clone the hive from `https://github.com/apache/hive`
2. checking out appropriate release
3. to get hadoop version execute 
```
mvn -q help:evaluate -Dexpression=hadoop.version -DforceStdout
```
4. to get tez version execute 
```
mvn -q help:evaluate -Dexpression=tez.version -DforceStdout
```

When the `buildAllDockerImages` task is run, the plugin reads this manifest file and for each
object, it creates a Docker image with the specified Hive version. This allows you to easily build
and publish Docker images for multiple versions of Hive to `quay.io/openlineage/hive`.


## Starting Hive using docker-compose

Just execute:

```shell
docker compose pull  # pull image from quay.io, can be skipped if image was build locally
docker compose down -v  # important to clear metastore & warehouse
docker compose up  # start container and show HiveServer2 logs
```

After HiveServer2 is started, connect using ``beeline`` cli or JDBC (e.g. DBBeaver):

```shell
docker exec -it hive-docker-hive-1 bash
$ beeline -u "jdbc:hive2://localhost:10000"
```

Here you can run Hive queries. OpenLineage events will appear in HiveServer2 logs.

Container also exposes web UI http://localhost:10002 where you can see status of sessions and queries.
