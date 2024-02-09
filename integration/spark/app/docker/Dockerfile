ARG BASE_IMAGE_TAG

FROM bitnami/spark:${BASE_IMAGE_TAG} AS builder

USER root

# This is needed to verify the downloads
RUN install_packages gnupg2

WORKDIR /tmp/bitnami/pkg/cache

COPY spark.tgz .
COPY spark.tgz.asc .
COPY KEYS .

RUN export GNUPGHOME="$(mktemp -d)"; \
    gpg --batch --import KEYS; \
    gpg --batch --verify spark.tgz.asc spark.tgz; \
    gpgconf --kill all; \
    rm -rf "$GNUPGHOME";

RUN tar -xf spark.tgz --strip-components=1;

USER 1001

FROM bitnami/spark:${BASE_IMAGE_TAG} AS runtime

USER root

RUN rm -rf /opt/bitnami/spark/jars; \
    rm -rf /opt/bitnami/spark/examples;

COPY --from=builder /tmp/bitnami/pkg/cache/jars /opt/bitnami/spark/jars
COPY --from=builder /tmp/bitnami/pkg/cache/examples /opt/bitnami/spark/examples

RUN chown -R 1001:root /opt/bitnami/spark/jars /opt/bitnami/spark/examples

USER 1001
