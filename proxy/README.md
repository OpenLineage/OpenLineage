# OpenLineage Proxy Backend

A `backend` to proxy OpenLineage events to one or more event streams. By default, events are logged to the console.  

## Requirements

* [Java 11](https://openjdk.java.net/install)

## Building

To build the entire project run:

```bash
$ ./gradlew build
```

The executable can be found under `build/libs/`

## Configuration

To run the OpenLineage _proxy backend_, you will have to define `proxy.yml`. The configuration file is passed to the application and used to configure your proxy. Please copy [`proxy.example.yml`](proxy.example.yml):

```bash
$ cp proxy.example.yml proxy.yml
```

By default, the OpenLineage proxy uses the following ports:

* TCP port `8080` is available for the HTTP API server.
* TCP port `8081` is available for the admin interface.

> **Note:** All of the configuration settings in `proxy.yml` can be specified either in the configuration file or in an environment variable.

## Running the HTTP API Server

```bash
$ ./gradlew runShadow
```


