# openlineage-proxy

## Building

To build the entire project run:

```bash
$ ./gradlew shadowJar
```
The executable can be found under `build/libs/`

## Configuration

To run the OpenLineage proxy, you will have to define `proxy.yml`. The configuration file is passed to the application and used to specify your proxy connection details. Please copy [`proxy.example.yml`](proxy.example.yml):

```bash
$ cp proxy.example.yml proxy.yml
```

By default, the OpenLineage proxy uses the following ports:

* TCP port **`8080`** is available for the app server.
* TCP port **`8081`** is available for the admin interface.

> **Note:** All of the configuration settings in `proxy.yml` can be specified either in the configuration file or in an environment variable.

## Running the HTTP API Server

```bash
$ ./gradlew runShadow
```

