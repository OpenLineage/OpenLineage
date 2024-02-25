# OpenLineage Spark Docker Image Builder

This project is responsible for building and publishing Docker images for different versions of
Spark to `quay.io/openlineage/spark`. These images are used in the integration tests of the
OpenLineage Spark integration.

## Steps

1. Login to `quay.io` using the following command: `docker login quay.io`
2. Create a builder for the Docker images using the following
   command: `docker buildx create --use --name openlineage-spark-builder --driver docker-container`
3. Run the Gradle task to build and publish the Docker images: `./gradlew buildAllDockerImages`

## manifest.json

The `manifest.json` file is used to specify the different versions of Spark for which Docker images
should be built. Each object in the JSON array represents a different Spark version and contains the
following properties:

- `baseImageTag`: The tag of the base Docker image.
- `scalaBinaryVersion`: The version of Scala binary used.
- `sparkPgpKeys`: The URL to the PGP keys for verifying the downloaded Spark binaries.
- `sparkSourceBinaries`: The URL to the Spark source binaries.
- `sparkSourceBinariesAsc`: The URL to the asc file for verifying the downloaded Spark binaries.
- `sparkVersion`: The version of Spark.

When the `buildAllDockerImages` task is run, the plugin reads this manifest file and for each
object, it creates a Docker image with the specified Spark version. This allows you to easily build
and publish Docker images for multiple versions of Spark to `quay.io/openlineage/spark`.
