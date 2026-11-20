# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Python client with the nats extra, built with network access; dev/nats-offline/run.sh then runs
# the NATS tests from it on a network without internet access.
FROM docker.io/library/nats:2 AS nats

FROM docker.io/library/python:3.12-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends openssl \
 && rm -rf /var/lib/apt/lists/*
COPY --from=nats /nats-server /usr/local/bin/nats-server
RUN pip install --no-cache-dir uv
COPY client/python /src/client/python
WORKDIR /src/client/python
RUN uv pip install --system --no-cache -e ".[test,nats]"
CMD ["python", "-m", "pytest", "-p", "no:cacheprovider", "tests/test_nats.py"]
