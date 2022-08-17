# OpenLineage-python

To install from source, run:

```bash
$ python setup.py install
```

## Configuration
### Config file

Main way of configuring OpenLineage Client is by yaml file, which contains all the
details of how to connect to your OpenLineage backend. 

Config file is located by
1) looking at `OPENLINEAGE_CONFIG` environment variable
2) looking for file `openlineage.yml` at current working directory
3) looking for file `openlineage.yml` at `$HOME/.openlineage` directory.

Different ways of connecting to OpenLineage backend are supported 
by standarized `Transport` interface.  
This is example config that specifies `http` transport:

```yaml
transport:
  type: "http"
  url: "https://backend:5000"
  auth:
    type: "api_key"
    api_key: "f048521b-dfe8-47cd-9c65-0cb07d57591e"
```

`type` property is required. It can be one of the build-in transports, or custom one.
There are three build-in transports, `http`, `kafka` or `console`. 
Custom transports `type` is fully qualified class name that can be imported.

Rest of the properties are defined by particular transport.  
Specification of build-in ones are below.

#### HTTP

* `url` - required string parameter specifying
* `endpoint` - optional string parameter specifying endpoint to which events are sent. By default `api/v1/lineage`.
* `timeout` - optional float parameter specifying timeout when sending event. By default 5 seconds.
* `verify` optional boolean attribute specifying if client should verify TLS certificates of backend. By default true.
* `auth` - optional dictionary specifying authentication options. Type property is required.
    * `type`: required property if auth dictionary is set. Set to `api_key` or to fully qualified class name of your TokenProvider
    * `api_key`: if `api_key` type is set, it sets value at `Authentication` HTTP header as `Bearer` 

#### Kafka

* `config` - required string parameter - dictionary that contains Kafka producer config as in [Kafka producer config](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafka-client-configuration)
* `topic` - required string parameter - topic on what events will be send
* `flush` - optional boolean parameter - if it's set to True, then Kafka will flush after each event. By default set to true.


### Custom transport

To implement custom transport, follow instructions in `openlineage/client/transport/transport.py` file.

### Config as env vars

If there is no config file found, OpenLineage client looks at environment variables.  
This way of configuring supports only `http` transport, and only subset of it's config.

* `OPENLINEAGE_URL` - point to service which will consume OpenLineage events
* `OPENLINEAGE_API_KEY` - set if consumer of OpenLineage events requires `Bearer` authentication key

`OPENLINEAGE_URL` and `OPENLINEAGE_API_KEY` can also be set up manually when creating client instance.
