# OpenLineage-python

To install from source, run:

```bash
$ python setup.py install
```

## Configuration
### Config file

The most common way to configure the OpenLineage Client is by `.yaml` file, which contains all the
details of how to connect to your OpenLineage backend. 

The config file is located by:
1) looking at the `OPENLINEAGE_CONFIG` environment variable
2) looking for the `openlineage.yml` file in the current working directory
3) looking for the `openlineage.yml` file in the `$HOME/.openlineage` directory.

Different ways of connecting to OpenLineage backend are supported 
by the standarized `Transport` interface.  
This is an example config for specifying `http` transport:

```yaml
transport:
  type: "http"
  url: "https://backend:5000"
  auth:
    type: "api_key"
    api_key: "f048521b-dfe8-47cd-9c65-0cb07d57591e"
```

The `type` property is required. It can be one of the built-in transports or a custom one.
There are three built-in transports, `http`, `kafka` and `console`. 
Custom transports `type` is a fully qualified class name that can be imported.

The rest of the properties are defined by the particular transport.  
Specifications for the built-in options are below.

#### HTTP

* `url` - required string parameter.
* `endpoint` - optional string parameter specifying the endpoint to which events are sent. By default `api/v1/lineage`.
* `timeout` - optional float parameter specifying the timeout when sending events. By default 5 seconds.
* `verify` optional boolean attribute specifying if the client should verify TLS certificates of the backend. By default true.
* `auth` - optional dictionary specifying authentication options. The type property is required.
    * `type`: required property if an auth dictionary is set. Set to `api_key` or to the fully qualified class name of your TokenProvider.
    * `api_key`: if `api_key` type is set, it sets value at `Authentication` HTTP header as `Bearer`. 

#### Kafka

For KafkaTransport, `confluent-kafka` needs to be installed.
You can also install `pip install openlineage-python[kafka]`

* `config` - required string parameter. A dictionary that contains a Kafka producer config as in [Kafka producer config](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafka-client-configuration).
* `topic` - required string parameter. The topic on what events will be sent.
* `flush` - optional boolean parameter. If set to True, Kafka will flush after each event. By default true.


### Custom transport

To implement a custom transport, follow the instructions in the `openlineage/client/transport/transport.py` file.

### Config as env vars

If there is no config file found, the OpenLineage client looks at environment variables.  
This way of configuring the client supports only `http` transport, and only a subset of its config.

* `OPENLINEAGE_URL` - point to the service that will consume OpenLineage events.
* `OPENLINEAGE_API_KEY` - set if the consumer of OpenLineage events requires a `Bearer` authentication key.

`OPENLINEAGE_URL` and `OPENLINEAGE_API_KEY` can also be set up manually when creating a client instance.