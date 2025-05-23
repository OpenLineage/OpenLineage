# OpenLineage Hive Docker Image Builder

This project is responsible for building and publishing Docker images for different versions of
Hive to `quay.io/openlineage/hive`. These images are used in the integration tests of the
OpenLineage Hive integration.

## Steps

1. Login to `quay.io` using the following command: `docker login quay.io`
2. Create a builder for the Docker images using the following
   command: `docker buildx create --use --name openlineage-hive-builder --driver docker-container`
3. Run the Gradle task to build and publish the Docker images: `./gradlew buildAllDockerImages`

For testing you can use `./gradlew buildAllDockerImages -PskipPush=true` to only build locally for your system

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
