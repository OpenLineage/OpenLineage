# Fluentd and Openlineage

## Why Fluentd and Openlineage are a perfect match?

**NOTE:** Fluentd & Openlineage support should be considered experimental now which means it can be 
modified or removed in the future. 

Modern data collectors like (Fluentd, Logstash, Vector etc.) can be extremely useful when designing
production grade Openlineage architectures. Just to name a few of great features that can be achieved
with data collectors: 
 * A server-proxy in front of the Openlineage backend (like Marquez) to handle load spikes and buffer incoming events when backend is down (due to maintainance window).
 * The ability to copy the event into multiple backends like multiple HTTP backends, kafka or cloud object storage. Data collectors implement that out-of-the-box.

They have a great potential except for a single missing feature: the ability to parse OpenLineage 
events at HTTP input, validate their content and return potential errors directly to a client. Fortunately,
this missing feature can be implemented as a plugin. 

Fluentd integration has been chosen because:
 * It has a low footprint in terms of resource utilisation,
 * Plugins can be installed from local files (no need to register in plugin repository),

As a side effect fluentd integration can 

## Fluentd features

Some interesting fluentd features available on [official documentation](https://docs.fluentd.org/).

 * [Buffering/retrying parameters](https://docs.fluentd.org/output#buffering-retrying-parameters),
 * [Output Kafka plugin](https://docs.fluentd.org/output/kafka),
 * [Output S3 plugin](https://docs.fluentd.org/output/s3),
 * [Output copy plugin](https://docs.fluentd.org/output/copy),
 * [Output http plugin](https://docs.fluentd.org/output/http) with following options:
   * `retryable_response_codes` [link](https://docs.fluentd.org/output/http#retryable_response_codes) to specify backend codes that should cause a retry,
   * [Buffer configuration](https://docs.fluentd.org/configuration/buffer-section),
 * Fluentd official doc does not mention guarantees on event ordering except for `flush_thread_count` setting which is by default 1. 

## Fluent-plugin-openlineage-parser

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

#### RubyGems

[//]: # (```)

[//]: # ($ gem install fluent-plugin-openlineage-parser)

[//]: # (```)

[//]: # ()
[//]: # (#### Bundler)

[//]: # ()
[//]: # (Add following line to your Gemfile:)

[//]: # ()
[//]: # (```ruby)

[//]: # (gem "fluent-plugin-openlineage-parser")

[//]: # (```)

[//]: # (And then execute:)

```
$ bundle
```

Then run
```shell
 bundle info fluent-plugin-openlineage
```
to verify if a plugin is installed

## Future work

 * Register built parser library in Ruby repository, 
 * Run performance tests.