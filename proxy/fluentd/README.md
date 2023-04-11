# Fluentd and Openlineage

## Why Fluentd and Openlineage are a perfect match?

**Fluentd & Openlineage support is experimental and can be changed or removed in future releases.** 

Modern data collectors like (Fluentd, Logstash, Vector etc.) can be extremely useful when designing
production grade architectures for processing Openlineage events. 

They can be used for the features like:
 * A server-proxy in front of the Openlineage backend (like Marquez) to handle load spikes and buffer incoming events when backend is down (due to maintainance window).
 * The ability to copy the event into multiple backends like multiple HTTP backends, kafka or cloud object storage. Data collectors implement that out-of-the-box.

They have a great potential except for a single missing feature: *the ability to parse OpenLineage validate Openlineage events at HTTP input*.
This is important as one would like to get `Bad Request` response immediately when sending invalid Openlineage events to an endpoint.
Fortunately, this missing feature can be implemented as a plugin. 

We decided to implement Openlineage parser plugin for Fluentd because:
 * Fluentd has a low footprint in terms of resource utilisation and does not require JVM installed,
 * Fluentd plugins can be installed from local files (no need to register in plugin repository),

As a side effect, Fluentd integration can be also used as a Openlineage HTTP validation backend for 
development purpose.

## Fluentd features

Some interesting fluentd features available on [official documentation](https://docs.fluentd.org/).

 * [Buffering/retrying parameters](https://docs.fluentd.org/output#buffering-retrying-parameters),
 * Some useful output plugins:
   * [Output Kafka plugin](https://docs.fluentd.org/output/kafka),
   * [Output S3 plugin](https://docs.fluentd.org/output/s3),
   * [Output copy plugin](https://docs.fluentd.org/output/copy),
   * [Output http plugin](https://docs.fluentd.org/output/http) with following options like [retryable_response_codes](https://docs.fluentd.org/output/http#retryable_response_codes) to specify backend codes that should cause a retry,
   * [Buffer configuration](https://docs.fluentd.org/configuration/buffer-section).

Fluentd official documentation does not mention guarantees on event ordering. However, retrieving
Openlineage events and buffering in file/memory should be considered millisecond long operation, 
while any HTTP backend cannot guarantee ordering in such case. On the other hand, by default
the amount of threads to flush the buffer is set to 1 and configurable ([flush_thread_count](https://docs.fluentd.org/output#flush_thread_count)).

## Openlineage parser plugin

Openlineage-parser is a fluentd plugin that verifies if a JSON matches Openlineage schema. 

### Usage

Please refer to `Dockerfile` and `fluent.conf` to see how to build and install the plugin with
example usage scenario provided in `docker-compose.yml`. To run example setup go to `docker` directory and execute following command.

```shell
docker-compose up
```

After all the containers started send some http requests. 

```shell
curl -X POST \
-d '{"test":"test"}' \
-H 'Content-Type: application/json' \
http://localhost:9880/api/v1/lineage
```
In response, you should see following message:

`Openlineage validation failed: path "/": "run" is a required property, path "/": "job" is a required property, path "/": "eventTime" is a required property, path "/": "producer" is a required property, path "/": "schemaURL" is a required property`

Next send some valid requests:

```shell
curl -X POST \
-d "$(cat test-start.json)" \
-H 'Content-Type: application/json' \
http://localhost:9880/api/v1/lineage
```

```shell
curl -X POST \
-d "$(cat test-complete.json)" \
-H 'Content-Type: application/json' \
http://localhost:9880/api/v1/lineage
```

After that you should see entities in marquez http://localhost:3000/ in `my-namespace` namespace.

To clean up run
```shell
docker-compose down
```
### Development

To build dependencies: 
```shell
bundle install
bundle
```

To run the tests:
```shell
bundle exec rake test
```

#### Installation

The easiest way to install the plugin is to install external package `rusty_json_schema`, which 
can be done with: 
```shell
gem install rusty_json_schema
```
Once the external dependency is installed, a single ruby code file `parser_openlineage.rb` needs 
to be copied into fluentd plugins directory ([installing custom plugin](https://docs.fluentd.org/plugin-development#installing-custom-plugins)).

## Future work

 * Include running unit tests in CI, 
 * Ship parser as a gem.