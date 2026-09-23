# Offline NATS transport tests

Runs the Python and Java NATS transport tests in containers that have **no internet access**, next to
a JetStream-enabled NATS server. A passing run shows that:

- the tests need nothing beyond the dependencies baked into the images, and never reach the internet;
- they work against a NATS server in another container (`NATS_URL`), not only against a local
  `nats-server` binary.

```sh
dev/nats-offline/run.sh            # both clients
dev/nats-offline/run.sh python     # or: java
RUNTIME=docker dev/nats-offline/run.sh
```

`RUNTIME` defaults to [Apple `container`](https://github.com/apple/container); `docker` works too.

## How it works

1. **Build, with network access.** `python.Containerfile` installs the client with the `test` and
   `nats` extras. `java.Containerfile` runs the Gradle build once so the Gradle distribution, plugins and
   dependencies are cached in the image. Both images get `nats-server` (copied from the `nats:2`
   image) and `openssl`, for the tests that start their own servers with auth or TLS.
2. **Run, without network access.** `run.sh` creates an internal network (`--internal`), starts
   `nats:2 -js` on it, and runs each test image on the same network with `NATS_URL` pointing at that
   server. Java runs with `gradle --offline`, re-running only the NATS test tasks: the core client's
   `generateCode` task fetches every schema's published `$id` from `openlineage.io`, so it has to stay
   up to date from the build step. Before the tests, each container checks that the
   internet is unreachable and fails if it is not.

The build context is staged from the files git knows about under `spec/`, `client/python/` and
`client/java/`, so local virtualenvs and build output stay out of the images. It is staged under
`${XDG_CACHE_HOME:-~/.cache}` because Apple `container`'s builder sees an empty context in `/tmp`
or `/var/folders` without reporting an error.

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2026 contributors to the OpenLineage project
