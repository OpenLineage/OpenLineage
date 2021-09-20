# openlineage-proxy

## Building

To build the entire project run:

```bash
$ ./gradlew shadowJar
```
The executable can be found under `build/libs/`

## Configuration

To run Marquez, you will have to define `config.yml`. The configuration file is passed to the application and used to specify your database connection. When creating your database, we recommend calling it `marquez`. Please copy [`config.example.yml`](https://github.com/MarquezProject/marquez/blob/master/config.example.yml):

```bash
$ cp config.example.yml config.yml
```

You will then need to set the following environment variables (we recommend adding them to your `.bashrc`): **`POSTGRES_DB`**, **`POSTGRES_USER`**, and **`POSTGRES_PASSWORD`**.

By default, Marquez uses the following ports:

* TCP port **`8080`** is available for the app server.
* TCP port **`8081`** is available for the admin interface.

**Note:** All environment variables in `config.yml` are accessed with [`${VAR_NAME}`](https://www.dropwizard.io/1.3.5/docs/manual/core.html#environment-variables).

## Running the [Application](https://github.com/MarquezProject/marquez/blob/master/src/main/java/marquez/MarquezApp.java)

```bash
$ ./gradlew run --args 'server config.yml'
```

## Running with [Docker](./Dockerfile)

```
$ docker-compose up
```

Marquez listens on port **`5000`** for all API calls and port **`5001`** for the admin interface.
