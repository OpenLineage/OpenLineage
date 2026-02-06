---
sidebar_position: 2
title: Configuration
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


## Intro

The OpenLineage Python client supports four main configuration sections that control how events are emitted and what metadata is included:

1. **Transports** - Configures how events are sent to OpenLineage backends (HTTP, Kafka, File, Console, etc.)
2. **Facets** - Configures some facets (e.g., which environment variables are attached to events as facet)
3. **Filters** - Defines rules to selectively exclude certain events from being emitted
4. **Tags** - Configures custom tags added to jobs and runs entities as custom facet.

Configuration can be provided in several ways:

:::note
Configuration is read only at client creation time; any changes to configuration environment variables or 
the configuration file made after a client has been created will have no effect.  
:::

1. **Environment Variables** (Recommended) - See the [Environment Variables](#environment-variables) section below.

2. **YAML Configuration File** - Use an `openlineage.yml` file that contains all configuration details. The file can be located in three ways:
   - Set the `OPENLINEAGE_CONFIG` environment variable to the file path: `OPENLINEAGE_CONFIG=path/to/my_config.yml`
   - Place an `openlineage.yml` file in the current working directory
   - Place an `openlineage.yml` file under `.openlineage/` in the user's home directory (`~/.openlineage/openlineage.yml`)

3. **Python Code** - Pass configuration directly to the `OpenLineageClient` constructor using the `config` parameter

The configuration precedence is as follows:
1. Configuration passed to the client constructor
2. YAML config file (if found)
3. Environment variables with the `OPENLINEAGE__` prefix
4. Legacy environment variables for [HTTP transport](#http-transport-configuration-with-environment-variables)

If no configuration is found, `ConsoleTransport` is used by default, and events are printed to the console.

## Environment Variables

All variables (apart from [Meta Variables](#meta-variables)) that affect the client configuration start with the prefix `OPENLINEAGE__`, 
followed by nested keys separated by double underscores (`__`).

1. Prefix Requirement: All environment variables must begin with `OPENLINEAGE__`.
2. Sections Separation: Configuration sections are separated using double underscores `__` to form the hierarchy.
3. Lowercase Conversion: Environment variable values are automatically converted to lowercase.
4. JSON String Support: You can pass a JSON string at any level of the configuration hierarchy, which will be merged into the final configuration structure.
5. Hyphen Restriction: Since environment variable names cannot contain `-` (hyphen), if a name strictly requires a hyphen, use a JSON string as the value of the environment variable.
6. Precedence Rules:
* Top-level keys have precedence and will not be overwritten by more nested entries.
* For example, `OPENLINEAGE__TRANSPORT='{..}'` will not have its keys overwritten by `OPENLINEAGE__TRANSPORT__AUTH__KEY='key'`.

### Examples

<Tabs groupId="configs">
<TabItem value="basic" label="Basic Example">

Setting following environment variables:

```sh
OPENLINEAGE__TRANSPORT__TYPE=http
OPENLINEAGE__TRANSPORT__URL=http://localhost:5050
OPENLINEAGE__TRANSPORT__ENDPOINT=/api/v1/lineage
OPENLINEAGE__TRANSPORT__AUTH='{"type":"api_key", "apiKey":"random_token"}'
OPENLINEAGE__TRANSPORT__COMPRESSION=gzip
```

is equivalent to passing following YAML configuration:
```yaml
transport:
  type: http
  url: http://localhost:5050
  endpoint: api/v1/lineage
  auth:
    type: api_key
    apiKey: random_token
  compression: gzip
```
</TabItem>

<TabItem value="composite" label="Composite Example">

Setting following environment variables:

```sh
OPENLINEAGE__TRANSPORT__TYPE=composite
OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TYPE=http
OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__URL=http://localhost:5050
OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__ENDPOINT=/api/v1/lineage
OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__AUTH='{"type":"api_key", "apiKey":"random_token"}'
OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__COMPRESSION=gzip
OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__TYPE=console
```

is equivalent to passing following YAML configuration:
```yaml
transport:
  type: composite
  transports:
    first:
      type: http
      url: http://localhost:5050
      endpoint: api/v1/lineage
      auth:
        type: api_key
        apiKey: random_token
      compression: gzip
    second:
      type: console
```
</TabItem>

<TabItem value="precedence" label="Precedence Example">

Setting following environment variables:

```sh
OPENLINEAGE__TRANSPORT='{"type":"console"}'
OPENLINEAGE__TRANSPORT__TYPE=http
```

is equivalent to passing following YAML configuration:
```yaml
transport:
  type: console
```
</TabItem>

<TabItem value="kafka" label="Kafka Transport Example">

Setting following environment variables:

```sh
OPENLINEAGE__TRANSPORT__TYPE=kafka
OPENLINEAGE__TRANSPORT__TOPIC=my_topic
OPENLINEAGE__TRANSPORT__CONFIG='{"bootstrap.servers": "localhost:9092,another.host:9092", "acks": "all", "retries": 3}'
OPENLINEAGE__TRANSPORT__FLUSH=true
OPENLINEAGE__TRANSPORT__MESSAGE_KEY=some-value
```

is equivalent to passing following YAML configuration:
```yaml
transport:
  type: kafka
  topic: my_topic
  config:
    bootstrap.servers: localhost:9092,another.host:9092
    acks: all
    retries: 3
  flush: true
  message_key: some-value # this has been aliased to messageKey
```
</TabItem>

<TabItem value="file" label="File Transport with Remote Storage">

Setting following environment variables:

```sh
OPENLINEAGE__TRANSPORT__TYPE=file
OPENLINEAGE__TRANSPORT__LOG_FILE_PATH=s3://my-bucket/lineage/events.jsonl
OPENLINEAGE__TRANSPORT__APPEND=true
OPENLINEAGE__TRANSPORT__STORAGE_OPTIONS='{"key": "AKIAIOSFODNN7EXAMPLE", "secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "endpoint_url": "https://s3.amazonaws.com"}'
```

is equivalent to passing following YAML configuration:
```yaml
transport:
  type: file
  log_file_path: s3://my-bucket/lineage/events.jsonl
  append: true
  storage_options:
    key: AKIAIOSFODNN7EXAMPLE
    secret: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    endpoint_url: https://s3.amazonaws.com
```
</TabItem>

</Tabs>

### Meta variables
There are few variables that do not follow the above pattern (mostly due to legacy reasons): 

| Name                       | Description                                                       | Example                 | Since  |
|----------------------------|-------------------------------------------------------------------|-------------------------|--------|
| OPENLINEAGE_CONFIG         | The path to the YAML configuration file                           | path/to/openlineage.yml |        |
| OPENLINEAGE_CLIENT_LOGGING | Logging level of OpenLineage client and its child modules         | DEBUG                   |        |
| OPENLINEAGE_DISABLED       | When `true`, OpenLineage will not emit events (default: false)    | false                   | 0.9.0  |


### Legacy syntax

#### Http Transport

For backwards compatibility, the simplest HTTP transport configuration, with only a subset of its config, can be done with environment variables
(all other transport types are only configurable with full config). This setup can be done with the following environment variables:

- `OPENLINEAGE_URL` (required, the URL to send lineage events to, example: https://myapp.com)
- `OPENLINEAGE_ENDPOINT` (optional, endpoint to which events are sent, default: `api/v1/lineage`, example: api/v2/events)
- `OPENLINEAGE_API_KEY` (optional, token included in the Authentication HTTP header as the Bearer, example: secret_token_123)

To facilitate switch to modern environment variables, aliases are dynamically created for certain variables like `OPENLINEAGE_URL`. 
If `OPENLINEAGE_URL` is set, it automatically translates into specific transport configurations
that can be used with Composite transport with `default_http` as the name of the HTTP transport.

Alias rules are following:
* If environment variable `OPENLINEAGE_URL`="http://example.com" is set, it would insert following environment variables:
```sh
OPENLINEAGE__TRANSPORT__TRANSPORTS__DEFAULT_HTTP__TYPE="http"
OPENLINEAGE__TRANSPORT__TRANSPORTS__DEFAULT_HTTP__URL="http://example.com"
```
* Similarly if environment variable `OPENLINEAGE_API_KEY`="random_key" is set, it will be translated to:
```sh
OPENLINEAGE__TRANSPORT__TRANSPORTS__DEFAULT_HTTP__AUTH='{"type": "api_key", "apiKey": "random_key"}'
```
qually with environment variable `OPENLINEAGE_ENDPOINT`="api/v1/lineage", that translates to:
```sh
OPENLINEAGE__TRANSPORT__TRANSPORTS__DEFAULT_HTTP__ENDPOINT="api/v1/lineage"
```
* If one does not want to use aliased HTTP transport in Composite Transport, they can set `OPENLINEAGE__TRANSPORT__TRANSPORTS__DEFAULT_HTTP` to `{}`.

## Transports

### HTTP Transport

The HTTP transport provides synchronous, blocking event emission. This is the default transport implementation suitable for most use cases where immediate event delivery and error handling are preferred.

#### Configuration

- `type` - string, must be `"http"`. Required.
- `url` - string, base url for HTTP requests. Required.
- `endpoint` - string specifying the endpoint to which events are sent, appended to `url`. Optional, default: `api/v1/lineage`.
- `timeout` - float specifying timeout (in seconds) value used while connecting to server. Optional, default: `5`.
- `verify` - boolean specifying whether the client should verify TLS certificates from the backend. Optional, default: `true`.
- `auth` - dictionary specifying authentication options. Optional, by default no authorization is used. If set, requires the `type` property.
  - `type` - string specifying value for one of the out-of-the-box available authentication methods (`api_key` or `jwt`), or the fully qualified class name of your TokenProvider. Required if `auth` is provided.
  - Configuration options for `api_key` authentication:
    - `apiKey` - string setting the Authentication HTTP header as the Bearer. Required if `type` is `api_key`.
  - Configuration options for `jwt` authentication are documented in the [JWT Token Provider](#jwt-token-provider) section.
- `compression` - string, name of algorithm used by HTTP client to compress request body. Optional, default value `null`, allowed values: `gzip`. Added in v1.13.0.
- `custom_headers` - dictionary of additional headers to be sent with each request. Optional, default: `{}`.
- `retry` - dictionary of additional configuration options for HTTP retries. Added in v1.33.0. Defaults are below; those are non-exhaustive options, but the ones that are set by default.
  - `total` - total number of retries to be attempted. Default is `5`.
  - `read` - number of retries to be attempted on read errors. Default is `5`.
  - `connect` - number of retries to be attempted on connection errors. Default is `5`.
  - `backoff_factor` - a backoff factor to apply between attempts after the second try, default is `0.3`.
  - `status_forcelist` - a set of integer HTTP status codes that we should force a retry on, default is `[500, 502, 503, 504]`.
  - `allowed_methods` - a set of HTTP methods that we should retry on, default is `["HEAD", "POST"]`.

#### Behavior

Events are serialized to JSON, and then are sent as HTTP POST request with `Content-Type: application/json`. Events are sent immediately and the call blocks until completion. Uses httpx with built-in retry support and raises exceptions on failure.

#### Examples

<Tabs groupId="integrations">
<TabItem value="env-vars" label="Environment Variables">

```sh
OPENLINEAGE__TRANSPORT__TYPE=http
OPENLINEAGE__TRANSPORT__URL=https://backend:5000
OPENLINEAGE__TRANSPORT__ENDPOINT=api/v1/lineage
OPENLINEAGE__TRANSPORT__TIMEOUT=5
OPENLINEAGE__TRANSPORT__AUTH__TYPE=api_key
OPENLINEAGE__TRANSPORT__AUTH__APIKEY=f048521b-dfe8-47cd-9c65-0cb07d57591e
OPENLINEAGE__TRANSPORT__COMPRESSION=gzip
OPENLINEAGE__TRANSPORT__RETRY='{"total": 5, "read": 5, "connect": 5, "backoff_factor": 0.3, "status_forcelist": [500, 502, 503, 504], "allowed_methods": ["HEAD", "POST"]}'
```

</TabItem>
<TabItem value="env-var-single" label="Single Environment Variable">

```sh
OPENLINEAGE__TRANSPORT='{"type": "http", "url": "https://backend:5000", "endpoint": "api/v1/lineage", "timeout": 5, "auth": {"type": "api_key", "apiKey": "f048521b-dfe8-47cd-9c65-0cb07d57591e"}, "compression": "gzip", "retry": {"total": 5, "read": 5, "connect": 5, "backoff_factor": 0.3, "status_forcelist": [500, 502, 503, 504], "allowed_methods": ["HEAD", "POST"]}}'
```

</TabItem>
<TabItem value="yaml" label="Yaml Config">

```yaml
transport:
  type: http
  url: https://backend:5000
  endpoint: api/v1/lineage
  timeout: 5
  verify: false
  auth:
    type: api_key
    apiKey: f048521b-dfe8-47cd-9c65-0cb07d57591e
  compression: gzip
  retry:
    total: 5
    read: 5
    connect: 5
    backoff_factor: 0.3
    status_forcelist: [500, 502, 503, 504]
    allowed_methods: ["HEAD", "POST"]
```

</TabItem>
<TabItem value="python" label="Python Code">

```python
from openlineage.client import OpenLineageClient
from openlineage.client.transport.http import ApiKeyTokenProvider, HttpConfig, HttpCompression, HttpTransport

http_config = HttpConfig(
  url="https://backend:5000",
  endpoint="api/v1/lineage",
  timeout=5,
  verify=False,
  auth=ApiKeyTokenProvider({"apiKey": "f048521b-dfe8-47cd-9c65-0cb07d57591e"}),
  compression=HttpCompression.GZIP,
)

client = OpenLineageClient(transport=HttpTransport(http_config))
```
</TabItem>

</Tabs>

#### JWT Token Provider

The `JwtTokenProvider` is an authentication provider that exchanges an API key for a JWT token via a POST endpoint. This is useful for services that require OAuth-style authentication where you need to obtain a token before making API requests.

##### Configuration

When using JWT authentication with HTTP transport, configure the `auth` section as follows:

- `type` - string, must be `"jwt"`. Required.
- `apiKey` - string, the API key used to obtain the JWT token. Required.
- `tokenEndpoint` - string, the URL endpoint for token generation. Required.
- `tokenFields` - list of strings, JSON field names to search for the token in the response. The provider tries each field in order. Optional, default: `["token", "access_token"]`.
- `expiresInField` - string, JSON field name containing the token expiration time in seconds. Optional, default: `"expires_in"`.
- `grantType` - string, OAuth grant type parameter sent in the token request. Optional, default: `"urn:ietf:params:oauth:grant-type:jwt-bearer"`.
- `responseType` - string, OAuth response type parameter sent in the token request. Optional, default: `"token"`.
- `tokenRefreshBuffer` - integer, number of seconds before token expiry to trigger a refresh. Optional, default: `120`.

##### Behavior

- The provider sends a POST request with URL-encoded form data containing the API key and OAuth parameters.
- The response is expected to be JSON containing the JWT token and optionally an expiration time.
- Tokens are cached and automatically refreshed before expiration (default: 120 seconds before expiry, configurable via `tokenRefreshBuffer`).
- If no expiration is provided in the response, the provider attempts to extract it from the JWT payload's `exp` claim.
- The provider supports multiple JSON field names for the token, trying each in order until a match is found.
- Field matching is case-insensitive and handles both snake_case and camelCase variations (e.g., `expires_in` matches `expiresIn`).

##### Examples

<Tabs groupId="integrations">
<TabItem value="env-vars" label="Environment Variables">

Standard OAuth configuration:

```sh
OPENLINEAGE__TRANSPORT__TYPE=http
OPENLINEAGE__TRANSPORT__URL=https://backend:5000
OPENLINEAGE__TRANSPORT__AUTH__TYPE=jwt
OPENLINEAGE__TRANSPORT__AUTH__API_KEY=your-api-key
OPENLINEAGE__TRANSPORT__AUTH__TOKEN_ENDPOINT=https://auth.example.com/token
```

IBM Cloud IAM configuration:

```sh
OPENLINEAGE__TRANSPORT__TYPE=http
OPENLINEAGE__TRANSPORT__URL=https://backend:5000
OPENLINEAGE__TRANSPORT__AUTH__TYPE=jwt
OPENLINEAGE__TRANSPORT__AUTH__API_KEY=your-ibm-api-key
OPENLINEAGE__TRANSPORT__AUTH__TOKEN_ENDPOINT=https://iam.cloud.ibm.com/identity/token
OPENLINEAGE__TRANSPORT__AUTH__GRANT_TYPE=urn:ibm:params:oauth:grant-type:apikey
OPENLINEAGE__TRANSPORT__AUTH__RESPONSE_TYPE=cloud_iam
```

</TabItem>
<TabItem value="yaml" label="Yaml Config">

Standard OAuth configuration:

```yaml
transport:
  type: http
  url: https://backend:5000
  auth:
    type: jwt
    apiKey: your-api-key
    tokenEndpoint: https://auth.example.com/token
```

With custom field names:

```yaml
transport:
  type: http
  url: https://backend:5000
  auth:
    type: jwt
    apiKey: your-api-key
    tokenEndpoint: https://auth.example.com/token
    tokenFields: ["access_token", "token"]
    expiresInField: expires_in
```

IBM Cloud IAM configuration:

```yaml
transport:
  type: http
  url: https://backend:5000
  auth:
    type: jwt
    apiKey: your-ibm-api-key
    tokenEndpoint: https://iam.cloud.ibm.com/identity/token
    grantType: urn:ibm:params:oauth:grant-type:apikey
    responseType: cloud_iam
```

</TabItem>
<TabItem value="python" label="Python Code">

Standard OAuth configuration:

```python
from openlineage.client import OpenLineageClient
from openlineage.client.transport.http import HttpConfig, HttpTransport, JwtTokenProvider

http_config = HttpConfig(
    url="https://backend:5000",
    auth=JwtTokenProvider({
        "apiKey": "your-api-key",
        "tokenEndpoint": "https://auth.example.com/token"
    })
)

client = OpenLineageClient(transport=HttpTransport(http_config))
```

IBM Cloud IAM configuration:

```python
from openlineage.client import OpenLineageClient
from openlineage.client.transport.http import HttpConfig, HttpTransport, JwtTokenProvider

http_config = HttpConfig(
    url="https://backend:5000",
    auth=JwtTokenProvider({
        "apiKey": "your-ibm-api-key",
        "tokenEndpoint": "https://iam.cloud.ibm.com/identity/token",
        "grantType": "urn:ibm:params:oauth:grant-type:apikey",
        "responseType": "cloud_iam"
    })
)

client = OpenLineageClient(transport=HttpTransport(http_config))
```

</TabItem>
</Tabs>

### Async HTTP Transport

The Async HTTP transport provides high-performance, non-blocking event emission with advanced queuing and ordering guarantees. Use this transport when you need high throughput or want to avoid blocking your application on lineage event delivery.

Async transport API is experimental, and can change over the next few releases.

#### Configuration

- `type` - string, must be `"async_http"` or use direct instantiation. Required.
- `url` - string, base url for HTTP requests. Required.
- `endpoint` - string specifying the endpoint to which events are sent, appended to `url`. Optional, default: `api/v1/lineage`.
- `timeout` - float specifying timeout (in seconds) value used while connecting to server. Optional, default: `5`.
- `verify` - boolean specifying whether the client should verify TLS certificates from the backend. Optional, default: `true`.
- `auth` - dictionary specifying authentication options. Optional, by default no authorization is used. If set, requires the `type` property.
  - `type` - string specifying value for one of the out-of-the-box available authentication methods (`api_key` or `jwt`), or the fully qualified class name of your TokenProvider. Required if `auth` is provided.
  - Configuration options for `api_key` authentication:
    - `apiKey` - string setting the Authentication HTTP header as the Bearer. Required if `type` is `api_key`.
  - Configuration options for `jwt` authentication are documented in the [JWT Token Provider](#jwt-token-provider) section.
- `compression` - string, name of algorithm used by HTTP client to compress request body. Optional, default value `null`, allowed values: `gzip`.
- `custom_headers` - dictionary of additional headers to be sent with each request. Optional, default: `{}`.
- `max_queue_size` - integer specifying maximum events in processing queue. Optional, default: `10000`.
- `max_concurrent_requests` - integer specifying maximum parallel HTTP requests. Optional, default: `100`.
- `retry` - dictionary of additional configuration options for HTTP retries. Added in v1.33.0. Defaults are below; those are non-exhaustive options, but the ones that are set by default.
  - `total` - total number of retries to be attempted. Default is `5`.
  - `read` - number of retries to be attempted on read errors. Default is `5`.
  - `connect` - number of retries to be attempted on connection errors. Default is `5`.
  - `backoff_factor` - a backoff factor to apply between attempts after the second try, default is `0.3`.
  - `status_forcelist` - a set of integer HTTP status codes that we should force a retry on, default is `[500, 502, 503, 504]`.
  - `allowed_methods` - a set of HTTP methods that we should retry on, default is `["HEAD", "POST"]`.

#### Behavior

Events are processed asynchronously with the following features:

- **Event Ordering Guarantees**: START events are sent before their corresponding COMPLETE, FAIL, or ABORT events
- **High Throughput**: Non-blocking event emission with configurable concurrent processing
- **Queue Management**: Bounded queue prevents memory exhaustion with configurable size
- **Advanced Error Handling**: Retry logic with exponential backoff for network and server errors
- **Event Tracking**: Real-time statistics on pending, successful, and failed events

#### Event Flow

1. Events are queued for processing (START events immediately, other events wait until corresponding START event is send)
2. Worker thread processes events using configurable parallelism
3. Successful START events trigger release of pending completion events
4. Event statistics are tracked and available via `get_stats()`

#### Additional Methods

- `wait_for_completion(timeout: float)` - Wait for all events to be processed with timeout. If the value passed is negative, wait until all events get processed.
- `get_stats()` - Get processing statistics (`{"pending": 0, "success": 10, "failed": 0}`)
- `close(timeout: float)` - Shutdown with timeout. Skip pending events if they are still processing after timeout. If the value passed is negative, wait until all events get processed.

#### Examples

<Tabs groupId="integrations">
<TabItem value="env-vars" label="Environment Variables">

```sh
OPENLINEAGE__TRANSPORT__TYPE=async_http
OPENLINEAGE__TRANSPORT__URL=https://backend:5000
OPENLINEAGE__TRANSPORT__ENDPOINT=api/v1/lineage
OPENLINEAGE__TRANSPORT__TIMEOUT=5
OPENLINEAGE__TRANSPORT__VERIFY=false
OPENLINEAGE__TRANSPORT__AUTH='{"type":"api_key", "apiKey":"f048521b-dfe8-47cd-9c65-0cb07d57591e"}'
OPENLINEAGE__TRANSPORT__COMPRESSION=gzip
OPENLINEAGE__TRANSPORT__MAX_QUEUE_SIZE=1000000
OPENLINEAGE__TRANSPORT__MAX_CONCURRENT_REQUESTS=100
OPENLINEAGE__TRANSPORT__RETRY='{"total": 5, "read": 5, "connect": 5, "backoff_factor": 0.3, "status_forcelist": [500, 502, 503, 504], "allowed_methods": ["HEAD", "POST"]}'
```

</TabItem>
<TabItem value="env-var-single" label="Single Environment Variable">

```sh
OPENLINEAGE__TRANSPORT='{"type": "async_http", "url": "https://backend:5000", "endpoint": "api/v1/lineage", "timeout": 5, "verify": false, "auth": {"type": "api_key", "apiKey": "f048521b-dfe8-47cd-9c65-0cb07d57591e"}, "compression": "gzip", "max_queue_size": 1000000, "max_concurrent_requests": 100, "retry": {"total": 5, "read": 5, "connect": 5, "backoff_factor": 0.3, "status_forcelist": [500, 502, 503, 504], "allowed_methods": ["HEAD", "POST"]}}'
```

</TabItem>
<TabItem value="yaml" label="Yaml Config">

```yaml
transport:
  type: async_http
  url: https://backend:5000
  endpoint: api/v1/lineage
  timeout: 5
  verify: false
  auth:
    type: api_key
    apiKey: f048521b-dfe8-47cd-9c65-0cb07d57591e
  compression: gzip
  max_queue_size: 1000000
  max_concurrent_requests: 100
  retry:
    total: 5
    read: 5
    connect: 5
    backoff_factor: 0.3
    status_forcelist: [500, 502, 503, 504]
    allowed_methods: ["HEAD", "POST"]
```

</TabItem>
<TabItem value="python" label="Python Code">

```python
from openlineage.client import OpenLineageClient
from openlineage.client.transport.async_http import ApiKeyTokenProvider, AsyncHttpConfig, HttpCompression, AsyncHttpTransport

async_config = AsyncHttpConfig(
  url="https://backend:5000",
  endpoint="api/v1/lineage",
  timeout=5,
  verify=False,
  auth=ApiKeyTokenProvider({"apiKey": "f048521b-dfe8-47cd-9c65-0cb07d57591e"}),
  compression=HttpCompression.GZIP,
  max_queue_size=1000000,
  max_concurrent_requests=100
)

client = OpenLineageClient(transport=AsyncHttpTransport(async_config))

# Emit events asynchronously
client.emit(start_event)      # Non-blocking
client.emit(complete_event)   # Waits for START success, then sent

# Wait for all events to complete
client.transport.wait_for_completion()
# Get processing statistics
stats = client.transport.get_stats()
print(f"Pending: {stats['pending']}, Success: {stats['success']}, Failed: {stats['failed']}")
# Graceful shutdown
client.close()
```
</TabItem>

</Tabs>

### Datadog Transport

The Datadog transport sends OpenLineage events to Datadog's observability platform with intelligent transport routing based on event characteristics. This transport combines both synchronous HTTP and asynchronous HTTP capabilities, automatically selecting the optimal transport method based on configurable rules.

#### Configuration

- `type` - string, must be `"datadog"`. Required.
- `apiKey` - string, Datadog API key for authentication. Can also be set via `DD_API_KEY` environment variable. Required.
- `site` - string, Datadog site endpoint. Can be one of the predefined sites or a custom URL. Can also be set via `DD_SITE` environment variable. Optional, default: `"datadoghq.com"`.
- `timeout` - float specifying timeout (in seconds) value used while connecting to server. Optional, default: `5.0`.
- `retry` - dictionary of additional configuration options for HTTP retries. Optional, same defaults as HTTP transport.
- `max_queue_size` - integer specifying maximum events in async processing queue. Optional, default: `10000`.
- `max_concurrent_requests` - integer specifying maximum parallel HTTP requests for async transport. Optional, default: `100`.
- `async_transport_rules` - dictionary mapping integration and job types to transport selection. Optional, default: `{"dbt": {"*": True}}`.

#### Predefined Datadog Sites

The transport supports the following predefined Datadog sites:
- `datadoghq.com`
- `us3.datadoghq.com`
- `us5.datadoghq.com`
- `datadoghq.eu`
- `ap1.datadoghq.com`
- `ap2.datadoghq.com`
- `ddog-gov.com`
- `datad0g.com`

You can also provide a custom URL for `site` if using a proxy or custom endpoint.

#### Async Transport Rules

The `async_transport_rules` configuration allows fine-grained control over which events use asynchronous transport vs synchronous HTTP transport. Rules are defined as a two-level dictionary:

```yaml
async_transport_rules:
  <integration>:
    <jobType>: <boolean>
```

First-level keys match against the `integration` field in `JobTypeJobFacet` Second-level keys match against the `jobType` field in `JobTypeJobFacet`.
Value `true` uses async transport, `false` or lack of value uses synchronous HTTP transport.
Use `"*"` to match all integrations or job types. All matching is case-insensitive.

When the mapping for some `integration` - `jobType` pair aren't provided, it will use synchronous HTTPTransport. 
If you want to send all events via async transport, use double wildcard configuration. It will force async transport even if the `JobTypeJobFacet` is not present.

```yaml
async_transport_rules:
  "*":
   "*": true
```

#### Transport Selection Examples

Given these rules:
```yaml
async_transport_rules:
  dbt:
    "*": true
  spark:
    batch_job: true
    streaming_job: false
  "*":
    ml_training: true
```

**Event routing behavior**:
- `integration="dbt", jobType="model"` → **Async** (matches `dbt → *`)
- `integration="spark", jobType="batch_job"` → **Async** (matches `spark → batch_job`)
- `integration="spark", jobType="streaming_job"` → **HTTP** (matches `spark → streaming_job`)
- `integration="flink", jobType="ml_training"` → **Async** (matches `* → ml_training`)
- `integration="kafka", jobType="consumer"` → **HTTP** (no matching rule)

#### Examples

<Tabs groupId="integrations">
<TabItem value="env-vars" label="Environment Variables">

```bash
OPENLINEAGE__TRANSPORT__TYPE=datadog
OPENLINEAGE__TRANSPORT__APIKEY=your-datadog-api-key
OPENLINEAGE__TRANSPORT__SITE=datadoghq.com
OPENLINEAGE__TRANSPORT__TIMEOUT=10
OPENLINEAGE__TRANSPORT__ASYNC_TRANSPORT_RULES='{"dbt": {"*": true}, "spark": {"batch_job": true, "streaming_job": false}, "airflow": {"*": true}}'
```

Or using DD environment variables:
```bash
OPENLINEAGE__TRANSPORT__TYPE=datadog
DD_API_KEY=your-datadog-api-key
DD_SITE=datadoghq.com
```

</TabItem>
<TabItem value="env-var-single" label="Single Environment Variable">

```bash
OPENLINEAGE__TRANSPORT='{"type": "datadog", "apiKey": "your-datadog-api-key", "site": "datadoghq.com", "timeout": 10, "max_queue_size": 5000, "max_concurrent_requests": 50, "async_transport_rules": {"dbt": {"*": true}, "spark": {"batch_job": true, "streaming_job": false}, "airflow": {"*": true}}, "retry": {"total": 5, "backoff_factor": 0.3, "status_forcelist": [500, 502, 503, 504]}}'
```

</TabItem>
<TabItem value="yaml" label="Yaml Config">

```yaml
transport:
  type: datadog
  apiKey: your-datadog-api-key
  site: datadoghq.com
  timeout: 10
  max_queue_size: 5000
  max_concurrent_requests: 50
  async_transport_rules:
    # All dbt events use async transport
    dbt:
      "*": true
    # Spark sql-level events use async, other use sync
    spark:
      sql: true
    # All Airflow events use async transport
    airflow:
      "*": true
    # Example configuration that sends all events via async transport
    "*":
      "*": true
  retry:
    total: 5
    backoff_factor: 0.3
    status_forcelist: [500, 502, 503, 504]
```

</TabItem>
<TabItem value="python" label="Python Code">

```python
from openlineage.client import OpenLineageClient
from openlineage.client.transport.datadog import DatadogConfig, DatadogTransport

datadog_config = DatadogConfig(
    apiKey="your-datadog-api-key",
    site="datadoghq.com",
    timeout=10.0,
    max_queue_size=5000,
    max_concurrent_requests=50,
    async_transport_rules={
        "dbt": {"*": True},
        "spark": {"sql": True},
        "airflow": {"*": True},
        "*": {"*": True}  # Send all events via async transport.
    },
    retry={
        "total": 5,
        "backoff_factor": 0.3,
        "status_forcelist": [500, 502, 503, 504]
    }
)

client = OpenLineageClient(transport=DatadogTransport(datadog_config))
```

</TabItem>

</Tabs>


### GCP Data Catalog Lineage

The GCP Data Catalog Lineage transport sends OpenLineage events to Google Cloud Data Catalog Lineage API with intelligent transport routing. This transport combines both synchronous and asynchronous capabilities, automatically selecting the optimal transport method based on configurable rules similar to the Datadog transport.

#### Configuration

- `type` - string, must be `"gcplineage"`. Required.
- `project_id` - string, GCP project ID where the lineage data will be stored. Required.
- `location` - string, GCP location (region) for the lineage service. Optional, default: `"us-central1"`.
- `credentials_path` - string, path to service account JSON credentials file. Optional, uses default credentials if not provided.
- `async_transport_rules` - dictionary mapping integration and job types to transport selection. Optional, default: `{"dbt": {"*": True}}`.

#### Authentication

The transport supports two authentication methods:

1. **Service Account Key File**: Provide the path to a JSON key file via `credentials_path`
2. **Default Credentials**: Uses Google Cloud SDK default credentials (recommended for production)

When using default credentials, ensure your environment has proper authentication configured:
- For local development: `gcloud auth application-default login`
- For production: Use service account attached to compute resources or workload identity

#### Async Transport Rules

The `async_transport_rules` configuration works identically to the Datadog transport, allowing fine-grained control over which events use asynchronous transport vs synchronous transport. Rules are defined as a two-level dictionary:

```yaml
async_transport_rules:
  <integration>:
    <jobType>: <boolean>
```

First-level keys match against the `integration` field in `JobTypeJobFacet`. Second-level keys match against the `jobType` field in `JobTypeJobFacet`.
Value `true` uses async transport, `false` or missing value uses synchronous transport.
Use `"*"` to match all integrations or job types. All matching is case-insensitive.

When no mapping is provided for an `integration` - `jobType` pair, it uses synchronous transport.
To send all events via async transport, use double wildcard configuration:

```yaml
async_transport_rules:
  "*":
   "*": true
```

#### Examples

<Tabs groupId="integrations">
<TabItem value="env-vars" label="Environment Variables">

```bash
OPENLINEAGE__TRANSPORT__TYPE=gcplineage
OPENLINEAGE__TRANSPORT__PROJECT_ID=my-gcp-project
OPENLINEAGE__TRANSPORT__LOCATION=us-central1
OPENLINEAGE__TRANSPORT__CREDENTIALS_PATH=/path/to/service-account.json
OPENLINEAGE__TRANSPORT__ASYNC_TRANSPORT_RULES='{"dbt": {"*": true}, "airflow": {"*": true}}'
```

</TabItem>
<TabItem value="env-var-single" label="Single Environment Variable">

```bash
OPENLINEAGE__TRANSPORT='{"type": "gcplineage", "project_id": "my-gcp-project", "location": "us-central1", "credentials_path": "/path/to/service-account.json", "async_transport_rules": {"dbt": {"*": true}, "airflow": {"*": true}}'
```

</TabItem>
<TabItem value="yaml" label="YAML Config">

```yaml
transport:
  type: gcplineage
  project_id: my-gcp-project
  location: us-central1
  credentials_path: /path/to/service-account.json
  async_transport_rules:
    # All dbt events use async transport
    dbt:
      "*": true
    # All Airflow events use async transport
    airflow:
      "*": true
```

</TabItem>
<TabItem value="python" label="Python Code">

```python
from openlineage.client import OpenLineageClient
from openlineage.client.transport.gcplineage import GCPLineageConfig, GCPLineageTransport

gcp_config = GCPLineageConfig(
    project_id="my-gcp-project",
    location="us-central1",
    credentials_path="/path/to/service-account.json",
    async_transport_rules={
        "dbt": {"*": True},
        "airflow": {"*": True}
    }
)

client = OpenLineageClient(transport=GCPLineageTransport(gcp_config))
```

</TabItem>

</Tabs>

#### Requirements

This transport requires the `google-cloud-datacatalog-lineage` package:

```bash
pip install google-cloud-datacatalog-lineage
```

#### Integration with Google Dataplex

Events sent via this transport will appear in Google Cloud Data Catalog and can be viewed through Google Dataplex for lineage visualization and metadata management.

### Console

This straightforward transport emits OpenLineage events directly to the console through a logger.
No additional configuration is required.

#### Configuration

- `type` - string, must be `"console"`. Required.

#### Behavior

Events are serialized to JSON. Then each event is logged with `INFO` level to logger with name `openlineage.client.transport.console`.

#### Notes

Be cautious when using the `DEBUG` log level, as it might result in double-logging due to the `OpenLineageClient` also logging.

#### Examples

<Tabs groupId="integrations">
<TabItem value="env-vars" label="Environment Variables">

```sh
OPENLINEAGE__TRANSPORT__TYPE=console
```

</TabItem>
<TabItem value="env-var-single" label="Single Environment Variable">

```sh
OPENLINEAGE__TRANSPORT='{"type": "console"}'
```

</TabItem>
<TabItem value="yaml" label="Yaml Config">

```yaml
transport:
  type: console
```

</TabItem>
<TabItem value="python" label="Python Code">

```python
from openlineage.client import OpenLineageClient
from openlineage.client.transport.console import ConsoleConfig, ConsoleTransport

console_config = ConsoleConfig()
client = OpenLineageClient(transport=ConsoleTransport(console_config))
```
</TabItem>

</Tabs>

### Kafka

Kafka transport requires `confluent-kafka` package to be additionally installed.
It can be installed also by specifying kafka client extension: `pip install openlineage-python[kafka]`

#### Configuration

- `type` - string, must be `"kafka"`. Required.
- `topic` - string specifying the topic on what events will be sent. Required.
- `config` - a dictionary containing a Kafka producer config as in [Kafka producer config](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafka-client-configuration). Required.
- `flush` - boolean specifying whether Kafka should flush after each event. Optional, default: `true`.
- `messageKey` - string, key for all Kafka messages produced by transport. Optional, default value described below. Added in v1.13.0.

  Default values for `messageKey` are:
  - `run:{rootJob.namespace}/{rootJob.name}` - for RunEvent with parent facet containing link to `root` job
  - `run:{parentJob.namespace}/{parentJob.name}` - for RunEvent with parent facet
  - `run:{job.namespace}/{job.name}` - for RunEvent
  - `job:{job.namespace}/{job.name}` - for JobEvent
  - `dataset:{dataset.namespace}/{dataset.name}` - for DatasetEvent

#### Behavior

- Events are serialized to JSON, and then dispatched to the Kafka topic.
- If `flush` is `true`, messages will be flushed to the topic after each event being sent.

#### Notes

It is recommended to provide `messageKey` if Job hierarchy is used. It can be any string, but it should be the same for all jobs in
hierarchy, like `Airflow task -> Spark application -> Spark task runs`.

#### Using with Airflow integration

There's a caveat for using `KafkaTransport` with Airflow integration. In this integration, a Kafka producer needs to be created 
for each OpenLineage event.
It happens due to the Airflow execution and plugin model, which requires us to send messages from worker processes.
These are created dynamically for each task execution.

#### Examples

<Tabs groupId="integrations">
<TabItem value="env-vars" label="Environment Variables">

```sh
OPENLINEAGE__TRANSPORT__TYPE=kafka
OPENLINEAGE__TRANSPORT__TOPIC=my_topic
OPENLINEAGE__TRANSPORT__CONFIG='{"bootstrap.servers": "localhost:9092,another.host:9092", "acks": "all", "retries": 3}'
OPENLINEAGE__TRANSPORT__FLUSH=true
OPENLINEAGE__TRANSPORT__MESSAGE_KEY=some-value
```

</TabItem>
<TabItem value="env-var-single" label="Single Environment Variable">

```sh
OPENLINEAGE__TRANSPORT='{"type": "kafka", "topic": "my_topic", "config": {"bootstrap.servers": "localhost:9092,another.host:9092", "acks": "all", "retries": 3}, "flush": true, "messageKey": "some-value"}'
```

</TabItem>
<TabItem value="yaml" label="Yaml Config">

```yaml
transport:
  type: kafka
  topic: my_topic
  config:
    bootstrap.servers: localhost:9092,another.host:9092
    acks: all
    retries: 3
  flush: true
  messageKey: some-value
```

</TabItem>
<TabItem value="python" label="Python Code">

```python
from openlineage.client import OpenLineageClient
from openlineage.client.transport.kafka import KafkaConfig, KafkaTransport

kafka_config = KafkaConfig(
  topic="my_topic",
  config={
    "bootstrap.servers": "localhost:9092,another.host:9092",
    "acks": "all",
    "retries": "3",
  },
  flush=True,
  messageKey="some",
)

client = OpenLineageClient(transport=KafkaTransport(kafka_config))
```
</TabItem>

</Tabs>

### File

Designed mainly for integration testing, the `FileTransport` emits OpenLineage events to a given file(s). Supports both local and remote filesystems through optional fsspec integration.

#### Configuration

- `type` - string, must be `"file"`. Required.
- `log_file_path` - string specifying the path of the file or file prefix (when `append` is true). Required.
- `append` - boolean, see *Behavior* section below. Optional, default: `false`.
- `storage_options` - dictionary, additional options passed to fsspec for authentication and configuration. Optional.
- `filesystem` - string, dotted import path to a custom filesystem class or instance. Optional, provides explicit control over the filesystem.
- `fs_kwargs` - dictionary, keyword arguments for constructing the filesystem when using `filesystem`. Optional.

#### Behavior

- If the target file is absent, it's created.
- If `append` is `true`, each event will be appended to a single file `log_file_path`, separated by newlines.
- If `append` is `false`, each event will be written to as separated file with name `{log_file_path}-{datetime}`.
- When using remote filesystems, the transport automatically handles authentication and connection management through fsspec.

#### Remote Filesystem Support

The File transport supports remote filesystems through [fsspec](https://filesystem-spec.readthedocs.io/), which provides a unified interface for various storage backends including:

- **Amazon S3** (`s3://`)
- **Google Cloud Storage** (`gcs://` or `gs://`)
- **Azure Blob Storage** (`az://`, `abfs://`)
- **HDFS** (`hdfs://`)
- **FTP/SFTP** (`ftp://`, `sftp://`)
- **HTTP** (`http://`, `https://`)

##### Installation

To use remote filesystems, install the fsspec extra:

```bash
pip install openlineage-python[fsspec]
```

##### Configuration Methods

**Auto-detection Configuration**: FSSpec automatically detects the protocol from URL schemes:

```yaml
transport:
  type: file
  log_file_path: s3://my-bucket/lineage/events.jsonl
  # Protocol auto-detected from s3:// scheme
  storage_options:
    key: your-access-key
    secret: your-secret-key
    endpoint_url: https://custom-s3-endpoint.com
```

**Explicit Filesystem Configuration**: Provide explicit control over the filesystem using the `filesystem` parameter. This supports three approaches:

1. **Filesystem Class**: Reference a filesystem class that will be instantiated with `fs_kwargs`
2. **Filesystem Instance**: Reference a pre-configured filesystem instance (ignores `fs_kwargs`)  
3. **Factory Function**: Reference a callable that returns a filesystem instance when called with `fs_kwargs`

```yaml
# Example: Filesystem class
transport:
  type: file
  log_file_path: s3://my-bucket/lineage/events.jsonl
  filesystem: s3fs.S3FileSystem
  fs_kwargs:
    key: your-access-key
    secret: your-secret-key
```

##### Append Mode Considerations

**Important**: Many cloud storage filesystems (S3, GCS, Azure) do not support reliable append operations. When append mode is requested but not supported by the underlying filesystem, these filesystems may silently switch to overwrite mode, potentially causing data loss.

**Recommendations for cloud storage**:

- Use `append: false` to create timestamped files for better reliability
- Test append behavior with your specific storage backend before production use
- Monitor file outputs to ensure expected behavior

```yaml
transport:
  type: file
  log_file_path: s3://my-bucket/lineage/events
  protocol: s3
  append: false  # Recommended for cloud storage (creates timestamped files)
  storage_options:
    key: your-access-key
    secret: your-secret-key
```

#### Examples

<Tabs groupId="integrations">
<TabItem value="env-vars-local" label="Environment Variables (Local File)">

```sh
OPENLINEAGE__TRANSPORT__TYPE=file
OPENLINEAGE__TRANSPORT__LOG_FILE_PATH=/path/to/your/file
OPENLINEAGE__TRANSPORT__APPEND=false
```

</TabItem>
<TabItem value="env-vars-s3" label="Environment Variables (S3)">

```sh
OPENLINEAGE__TRANSPORT__TYPE=file
OPENLINEAGE__TRANSPORT__LOG_FILE_PATH=s3://my-bucket/lineage/events.jsonl
OPENLINEAGE__TRANSPORT__APPEND=false
OPENLINEAGE__TRANSPORT__STORAGE_OPTIONS='{"key": "AKIAIOSFODNN7EXAMPLE", "secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "endpoint_url": "https://s3.amazonaws.com"}'
```

</TabItem>
<TabItem value="env-var-single-local" label="Single Environment Variable (Local File)">

```sh
OPENLINEAGE__TRANSPORT='{"type": "file", "log_file_path": "/path/to/your/file", "append": false}'
```

</TabItem>
<TabItem value="env-var-single-s3" label="Single Environment Variable (S3)">

```sh
OPENLINEAGE__TRANSPORT='{"type": "file", "log_file_path": "s3://my-bucket/lineage/events.jsonl", "append": false, "storage_options": {"key": "AKIAIOSFODNN7EXAMPLE", "secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "endpoint_url": "https://s3.amazonaws.com"}}'
```

</TabItem>
<TabItem value="yaml-local" label="YAML Config (Local File)">

```yaml
transport:
  type: file
  log_file_path: /path/to/your/file
  append: false
```

</TabItem>
<TabItem value="yaml-s3" label="YAML Config (Amazon S3)">

```yaml
transport:
  type: file
  log_file_path: s3://my-bucket/lineage/events.jsonl
  append: false  # Recommended for cloud storage
  storage_options:
    key: AKIAIOSFODNN7EXAMPLE
    secret: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    endpoint_url: https://s3.amazonaws.com
```

</TabItem>
<TabItem value="yaml-gcs" label="YAML Config (Google Cloud Storage)">

```yaml
transport:
  type: file
  log_file_path: gs://my-bucket/lineage/events.jsonl
  append: false  # Recommended for cloud storage
  storage_options:
    token: /path/to/service-account.json
    project: my-gcp-project
```

</TabItem>
<TabItem value="yaml-azure" label="Azure Blob Storage">

```yaml
transport:
  type: file
  log_file_path: az://container/lineage/events.jsonl
  append: false  # Recommended for cloud storage
  storage_options:
    account_name: mystorageaccount
    account_key: base64_encoded_key
```

</TabItem>
<TabItem value="yaml-custom-fs" label="Custom Filesystem">

```yaml
transport:
  type: file
  log_file_path: /custom/path/events.jsonl
  filesystem: mymodule.MyCustomFileSystem
  fs_kwargs:
    endpoint: https://custom-storage.example.com
    auth_token: custom_token_123
    timeout: 30
```

</TabItem>
<TabItem value="yaml-fs-instance" label="Filesystem Instance">

```yaml
transport:
  type: file
  log_file_path: s3://my-bucket/lineage/events.jsonl
  filesystem: mymodule.my_preconfigured_s3_instance
  # fs_kwargs ignored when using an instance
```

</TabItem>
<TabItem value="yaml-fs-factory" label="Filesystem Factory">

```yaml
transport:
  type: file
  log_file_path: s3://my-bucket/lineage/events.jsonl
  filesystem: mymodule.create_secure_s3_filesystem
  fs_kwargs:
    key: AKIAIOSFODNN7EXAMPLE
    secret: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    endpoint_url: https://custom-s3-endpoint.com
    use_ssl: true
```

</TabItem>
<TabItem value="python-local" label="Python Code (Local)">

```python
from openlineage.client import OpenLineageClient
from openlineage.client.transport.file import FileConfig, FileTransport

file_config = FileConfig(
    log_file_path="/path/to/your/file",
    append=False,
)

client = OpenLineageClient(transport=FileTransport(file_config))
```

</TabItem>
<TabItem value="python-s3" label="Python Code (S3)">

```python
from openlineage.client import OpenLineageClient
from openlineage.client.transport.file import FileConfig, FileTransport

file_config = FileConfig(
    log_file_path="s3://my-bucket/lineage/events.jsonl",
    append=True,
    storage_options={
        "key": "AKIAIOSFODNN7EXAMPLE",
        "secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "endpoint_url": "https://s3.amazonaws.com",
    },
)

client = OpenLineageClient(transport=FileTransport(file_config))
```

</TabItem>
<TabItem value="python-custom" label="Python Code (Custom FS)">

```python
from openlineage.client import OpenLineageClient
from openlineage.client.transport.file import FileConfig, FileTransport

file_config = FileConfig(
    log_file_path="/custom/path/events.jsonl",
    filesystem="s3fs.S3FileSystem",
    fs_kwargs={
        "key": "AKIAIOSFODNN7EXAMPLE",
        "secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "client_kwargs": {"region_name": "us-west-2"},
    },
)

client = OpenLineageClient(transport=FileTransport(file_config))
```

</TabItem>
<TabItem value="python-fs-instance" label="Python Code (FS Instance)">

```python
from openlineage.client import OpenLineageClient
from openlineage.client.transport.file import FileConfig, FileTransport
import s3fs

# Create filesystem instance directly
s3_fs = s3fs.S3FileSystem(
    key="AKIAIOSFODNN7EXAMPLE",
    secret="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    endpoint_url="https://s3.amazonaws.com"
)

file_config = FileConfig(
    log_file_path="s3://my-bucket/lineage/events.jsonl",
    filesystem="__main__.s3_fs",  # Reference to the instance
    # fs_kwargs are ignored when using an instance
)

client = OpenLineageClient(transport=FileTransport(file_config))
```

</TabItem>
<TabItem value="python-fs-factory" label="Python Code (FS Factory)">

```python
from openlineage.client import OpenLineageClient
from openlineage.client.transport.file import FileConfig, FileTransport

def create_custom_s3_filesystem(**kwargs):
    """Factory function that creates a customized S3 filesystem."""
    import s3fs
    
    # Apply custom defaults or modifications
    config = {
        "use_ssl": True,
        "s3_additional_kwargs": {"ServerSideEncryption": "AES256"},
        **kwargs  # Allow override via fs_kwargs
    }
    
    return s3fs.S3FileSystem(**config)

file_config = FileConfig(
    log_file_path="s3://my-bucket/lineage/events.jsonl",
    filesystem="__main__.create_custom_s3_filesystem",  # Reference to factory function
    fs_kwargs={
        "key": "AKIAIOSFODNN7EXAMPLE",
        "secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "endpoint_url": "https://custom-s3-endpoint.com",
    },
)

client = OpenLineageClient(transport=FileTransport(file_config))
```

</TabItem>
</Tabs>

### Composite

The `CompositeTransport` is designed to combine multiple transports, allowing event emission to several destinations. This is useful when events need to be sent to multiple targets, such as a logging system and an API endpoint. The events are delivered sequentially - one after another in a defined order.

#### Configuration

- `type` - string, must be "composite". Required.
- `transports` - a list or a map of transport configurations. Required.
- `continue_on_failure` - boolean flag, determines if the process should continue even when one of the transports fails. Default is `true`.
- `continue_on_success` - boolean flag, determines if the process should continue when one of the transports succeeds. Default is `true`.
- `sort_transports` - boolean flag, determines if transports should be sorted by `priority` before emission. Default is `false`.

#### Behavior

- The configured transports will be initialized and used in sequence to emit OpenLineage events.
- If `continue_on_failure` is set to `false`, a failure in one transport will stop the event emission process, and an exception will be raised.
- If `continue_on_failure` is `true`, the failure will be logged and the process will continue allowing the remaining transports to still send the event.
- If `continue_on_success` is set to `false`, a success of one transport will stop the event emission process. This is useful if you want to deliver events to at most one backend, and only fallback to other backends in case of failure.
- If `continue_on_success` is set to `true`, the success will be logged and the process will continue allowing the remaining transports to send the event.

#### Transport Priority
Each transport in the `transports` configuration can include an optional `priority` field (integer). 
When `sort_transports` is `true`, transports are sorted by priority in descending order (higher priority values are processed first). 
Transports without a priority field default to priority 0.


#### Notes for Multiple Transports
The composite transport can be used with any OpenLineage transport (e.g. `HttpTransport`, `KafkaTransport`, etc).

The `transports` configuration can be provided in two formats:

1. A list of transport configurations, where each transport may optionally include a `name` field.
2. A map of transport configurations, where the key acts as the name for each transport.
The map format is particularly useful for configurations set via environment variables.

##### Why are transport names used?
Transport names are not required for basic functionality. Their primary purpose is to enable configuration of composite transports via environment variables, which is only supported when names are defined.

#### Examples

<Tabs groupId="integrations">
<TabItem value="env-vars" label="Environment Variables">

```sh
OPENLINEAGE__TRANSPORT__TYPE = composite
OPENLINEAGE__TRANSPORT__CONTINUE_ON_FAILURE = true
OPENLINEAGE__TRANSPORT__CONTINUE_ON_SUCCESS = true
OPENLINEAGE__TRANSPORT__SORT_TRANSPORTS = true

# First transport - transform with http
OPENLINEAGE__TRANSPORT__TRANSPORTS__MY_FIRST_TRANSPORT_NAME__TYPE = transform
OPENLINEAGE__TRANSPORT__TRANSPORTS__MY_FIRST_TRANSPORT_NAME__PRIORITY = 1
OPENLINEAGE__TRANSPORT__TRANSPORTS__MY_FIRST_TRANSPORT_NAME__TRANSFORMER_CLASS = openlineage.client.transport.transform.JobNamespaceReplaceTransformer
OPENLINEAGE__TRANSPORT__TRANSPORTS__MY_FIRST_TRANSPORT_NAME__TRANSFORMER_PROPERTIES = '{"new_job_namespace": "new_namespace_value"}'
OPENLINEAGE__TRANSPORT__TRANSPORTS__MY_FIRST_TRANSPORT_NAME__TRANSPORT__TYPE = http
OPENLINEAGE__TRANSPORT__TRANSPORTS__MY_FIRST_TRANSPORT_NAME__TRANSPORT__URL = http://backend:5000
OPENLINEAGE__TRANSPORT__TRANSPORTS__MY_FIRST_TRANSPORT_NAME__TRANSPORT__ENDPOINT = api/v1/lineage
OPENLINEAGE__TRANSPORT__TRANSPORTS__MY_FIRST_TRANSPORT_NAME__TRANSPORT__AUTH__TYPE = api_key
OPENLINEAGE__TRANSPORT__TRANSPORTS__MY_FIRST_TRANSPORT_NAME__TRANSPORT__AUTH__API_KEY = 1500100900

# Second transport - http 
OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__TYPE = http
OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__PRIORITY = 0
OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__URL = http://another-backend:5000
OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__ENDPOINT = another/endpoint/v2
OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__AUTH__TYPE = api_key
OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__AUTH__API_KEY = bf6128d06dc2
```
</TabItem>
<TabItem value="env-var-single" label="Single Environment Variable">

```sh
OPENLINEAGE__TRANSPORT='{"type": "composite", "continue_on_failure": true, "continue_on_success": true, "sort_transports": true, "transports": {"MY_FIRST_TRANSPORT_NAME": {"type": "transform", "priority": 1, "transformer_class": "openlineage.client.transport.transform.JobNamespaceReplaceTransformer", "transformer_properties": {"new_job_namespace": "new_namespace_value"}, "transport": {"type": "http", "url": "http://backend:5000", "endpoint": "api/v1/lineage", "auth": {"type": "api_key", "apiKey": "1500100900"}}}, "SECOND": {"type": "http", "priority": 0, "url": "http://another-backend:5000", "endpoint": "another/endpoint/v2", "auth": {"type": "api_key", "apiKey": "bf6128d06dc2"}}}}'
```

</TabItem>
<TabItem value="yaml-list" label="YAML Config (List)">

```yaml
transport:
  type: composite
  continue_on_failure: true
  continue_on_success: true
  sort_transports: false
  transports:
    - type: http
      url: http://example.com/api
      name: my_http
    - type: http
      url: http://localhost:5000
      endpoint: /api/v1/lineage
```

</TabItem>
<TabItem value="yaml-map" label="YAML Config (Map)">

```yaml
transport:
  type: composite
  continue_on_failure: true
  continue_on_success: true
  sort_transports: true
  transports:
    my_http:
      type: http
      url: http://example.com/api
    local_http:
      type: http
      url: http://localhost:5000
      endpoint: /api/v1/lineage
      priority: 10
```

</TabItem>
<TabItem value="python" label="Python Code">

```python
from openlineage.client import OpenLineageClient
from openlineage.client.transport.composite import CompositeTransport, CompositeConfig

config = CompositeConfig.from_dict(
        {
            "type": "composite",
            "continue_on_failure": True,
            "continue_on_success": True,
            "sort_transports": True,
            "transports": [
                {
                    "type": "kafka",
                    "config": {"bootstrap.servers": "localhost:9092"},
                    "topic": "random-topic",
                    "messageKey": "key",
                    "flush": False,
                },
                {"type": "console", "priority": 1},
            ],
        },
    )
client = OpenLineageClient(transport=CompositeTransport(config))
```

</TabItem>

</Tabs>

### Transform

The `TransformTransport` is designed to enable event manipulation before emitting the event. 
Together with `CompositeTransport`, it can be used to send different events into multiple backends.

#### Configuration

- `type` - string, must be "transform". Required.
- `transport` - Transport configuration to emit modified events. Required.
- `transformer_class` - class name of the event transformer. Class has to implement `openlineage.client.transports.transform.EventTransformer` interface and be a fully qualified class name that can be imported. Required.
- `transformer_properties` - Extra properties to be passed as `properties` kwarg into `transformer_class` constructor. Optional, default is `{}`.

#### Behavior

- The configured `transformer_class` will be used to alter events before the emission.
- Modified events will be passed into the configured `transport` for further processing.
- If transformation fails, event emission will be skipped.
- If modified event is None, event emission will be skipped.

#### `EventTransformer` interface

```python
from __future__ import annotations

from typing import Any
from openlineage.client.client import Event

class EventTransformer:
    def __init__(self, properties: dict[str, Any]) -> None:
        self.properties = properties

    def transform(self, event: Event) -> Event | None:
        raise NotImplementedError
```

#### Examples

<Tabs groupId="integrations">
<TabItem value="env-vars" label="Environment Variables">

```sh
OPENLINEAGE__TRANSPORT__TYPE = transform

# Transformer
OPENLINEAGE__TRANSPORT__TRANSFORMER_CLASS = openlineage.client.transport.transform.JobNamespaceReplaceTransformer
OPENLINEAGE__TRANSPORT__TRANSFORMER_PROPERTIES = '{"new_job_namespace": "new_namespace"}'

# Transport
OPENLINEAGE__TRANSPORT__TRANSPORT__TYPE = http
OPENLINEAGE__TRANSPORT__TRANSPORT__URL = http://backend:5000
OPENLINEAGE__TRANSPORT__TRANSPORT__ENDPOINT = api/v1/lineage
OPENLINEAGE__TRANSPORT__TRANSPORT__VERIFY = false

# Transport Auth
OPENLINEAGE__TRANSPORT__TRANSPORT__AUTH__TYPE = api_key
OPENLINEAGE__TRANSPORT__TRANSPORT__AUTH__API_KEY = 1500100900

# Transport Compression
OPENLINEAGE__TRANSPORT__TRANSPORT__COMPRESSION = gzip

# Transport Retry settings
OPENLINEAGE__TRANSPORT__TRANSPORT__RETRY__TOTAL = 7
OPENLINEAGE__TRANSPORT__TRANSPORT__RETRY__CONNECT = 3
OPENLINEAGE__TRANSPORT__TRANSPORT__RETRY__READ = 2
OPENLINEAGE__TRANSPORT__TRANSPORT__RETRY__STATUS = 5
OPENLINEAGE__TRANSPORT__TRANSPORT__RETRY__OTHER = 1
OPENLINEAGE__TRANSPORT__TRANSPORT__RETRY__ALLOWED_METHODS = '["POST"]'
OPENLINEAGE__TRANSPORT__TRANSPORT__RETRY__STATUS_FORCELIST = [500, 502, 503, 504]
OPENLINEAGE__TRANSPORT__TRANSPORT__RETRY__BACKOFF_FACTOR = 0.5
OPENLINEAGE__TRANSPORT__TRANSPORT__RETRY__RAISE_ON_REDIRECT = false
OPENLINEAGE__TRANSPORT__TRANSPORT__RETRY__RAISE_ON_STATUS = false
```
</TabItem>
<TabItem value="env-var-single" label="Single Environment Variable">

```sh
OPENLINEAGE__TRANSPORT='{"type": "transform", "transformer_class": "openlineage.client.transport.transform.JobNamespaceReplaceTransformer", "transformer_properties": {"new_job_namespace": "new_namespace"}, "transport": {"type": "http", "url": "http://backend:5000", "endpoint": "api/v1/lineage", "verify": false, "auth": {"type": "api_key", "apiKey": "1500100900"}, "compression": "gzip", "retry": {"total": 7, "connect": 3, "read": 2, "status": 5, "other": 1, "allowed_methods": ["POST"], "status_forcelist": [500, 502, 503, 504], "backoff_factor": 0.5, "raise_on_redirect": false, "raise_on_status": false}}}'
```

</TabItem>
<TabItem value="yaml" label="YAML Config">

```yaml
transport:
  type: transform
  transformer_class: openlineage.client.transport.transform.JobNamespaceReplaceTransformer
  transformer_properties:
    new_job_namespace: new_value
  transport:
    type: http
    url: https://backend:5000
    endpoint: api/v1/lineage
    timeout: 5
    verify: false
    auth:
      type: api_key
      apiKey: f048521b-dfe8-47cd-9c65-0cb07d57591e
    compression: gzip
    retry:
      total: 5
      read: 5
      connect: 5
      backoff_factor: 0.3
      status_forcelist: [500, 502, 503, 504]
      allowed_methods: ["HEAD", "POST"]
```

</TabItem>
<TabItem value="python" label="Python Code">

```python
from openlineage.client import OpenLineageClient
from openlineage.client.transport.transform import TransformTransport, TransformConfig

transform_config = TransformConfig(
    transport={
        "type": "http",
        "url": "http://backend:5000",
        "endpoint": "api/v1/lineage",
        "verify": False,
        "auth": {
            "type": "api_key",
            "api_key": "1500100900",
        },
        "compression": "gzip",
        "retry": {
            "total": 7,
            "connect": 3,
            "read": 2,
            "status": 5,
            "other": 1,
            "allowed_methods": ["POST"],
            "status_forcelist": [500, 502, 503, 504],
            "backoff_factor": 0.5,
            "raise_on_redirect": False,
            "raise_on_status": False,
        },
    },
    transformer_class="openlineage.client.transport.transform.JobNamespaceReplaceTransformer",
    transformer_properties={"new_job_namespace": "new_namespace"}
)

client = OpenLineageClient(transport=TransformTransport(transform_config))
```

</TabItem>

</Tabs>

### Amazon DataZone

The `AmazonDataZoneTransport` requires `boto3` package to be additionally installed. It can be done via `pip install openlineage-python[datazone]`. This transport will send event to DataZone / SageMaker Unified Studio domain.

#### Configuration

- `type` - string, must be `"amazon_datazone_api"`. Required.
- `domainId` - string, specifies the DataZone / SageMaker Unified Studio domain id. The lineage events will be then sent to the following domain. Required.
- `region` - string. When provided, the DataZone client will be configured to use this specific region. If endpointOverride is also provided, this value is not used. Optional, default: None (uses AWS SDK default region resolution).
- `endpointOverride` - string, overrides the default HTTP endpoint for Amazon DataZone client.
  Default value will be set by AWS SDK to [following endpoints](https://docs.aws.amazon.com/general/latest/gr/datazone.html#datazone_region) based on the region.
  Optional, default: None

#### Behavior

- Events are serialized to JSON, and then dispatched to the `DataZone` / `SageMaker Unified Studio` endpoint.


#### Examples

<Tabs groupId="integrations">
<TabItem value="env-vars" label="Environment Variables">

```sh
OPENLINEAGE__TRANSPORT__TYPE=amazon_datazone_api
OPENLINEAGE__TRANSPORT__DOMAINID=dzd-domain-id
```

</TabItem>
<TabItem value="env-var-single" label="Single Environment Variable">

```sh
OPENLINEAGE__TRANSPORT='{"type": "amazon_datazone_api", "domainId": "dzd-domain-id"}'
```

</TabItem>
<TabItem value="yaml" label="YAML Config">

```yaml
transport:
  type: amazon_datazone_api
  domainId: dzd-domain-id
```

</TabItem>
<TabItem value="python" label="Python Code">

```python
from openlineage.client import OpenLineageClient
from openlineage.client.transport.amazon_datazone import AmazonDataZoneTransport, AmazonDataZoneConfig

datazone_config = AmazonDataZoneConfig(
  domainId="dzd-domain-id",
)

client = OpenLineageClient(transport=AmazonDataZoneTransport(datazone_config))
```

</TabItem>

</Tabs>

### Custom Transport Type

To implement a custom transport, follow the instructions in [`transport.py`](https://github.com/OpenLineage/OpenLineage/blob/main/client/python/openlineage/client/transport/transport.py).

The `type` property (required) must be a fully qualified class name that can be imported.

## Attaching environment variables

### Configuration

Environment variables can be included in OpenLineage events, as a facet called `EnvironmentVariablesRunFacet`. 
This feature allows you to specify the names of environment variables that should be collected and attached to each emitted event.

To enable this, configure the `environment_variables` option within the `facets` section of the configuration.

:::note
While the configuration is read only at client creation time (so the environment variables names can't be changed after 
a client has been created) - the value of the variables will be read and appended to the event at the time of event emission.
:::

### Examples

<Tabs groupId="env-vars-run-facet">
<TabItem value="env-vars" label="Environment Variables">

```sh
OPENLINEAGE__FACETS__ENVIRONMENT_VARIABLES='["VAR1", "VAR2"]'
```

</TabItem>
<TabItem value="yaml" label="YAML Config">

```yaml
facets:
  environment_variables:
    - VAR1
    - VAR2
```

</TabItem>
<TabItem value="python" label="Python Code">

```python
from openlineage.client import OpenLineageClient

config = {
    "environment_variables": ["VAR1", "VAR2"],
    "transport": {"type": "console"}
}
client = OpenLineageClient(config=config)
```

</TabItem>
</Tabs>


## Filters

Filters allow you to selectively prevent certain events from being emitted based on job name matching. 
Multiple filters can be configured, and if any filter matches, the event will be filtered out.

To enable this, configure the `filters` section of the configuration with separate dictionaries for each filter.

### Configuration

- `type` - string, must be `"exact"` or `"regex"`. Required.
- `match` - string, exact job name to match. Required if `type` is `"exact"`.
- `regex` - string, regular expression pattern to match. Required if `type` is `"regex"`.


:::note
- Filters only work on `RunEvent`.  
- Regular expressions use Python's `re.match()` function.
:::

### Examples

<Tabs groupId="filters">
<TabItem value="env-vars" label="Environment Variables">

```sh
OPENLINEAGE__FILTERS='[{"type": "exact", "match": "specific_job"}, {"type": "regex", "regex": "^temp_.*|.*_temp$"}]'
```

</TabItem>
<TabItem value="yaml" label="YAML Config">

```yaml
filters:
  - type: exact
    match: test_job
  - type: regex
    regex: ^temp_.*$
transport:
  type: console
```

</TabItem>
<TabItem value="python" label="Python Code">

```python
from openlineage.client import OpenLineageClient

config = {
    "filters": [
        {"type": "exact", "match": "test_job"},
        {"type": "regex", "regex": "^temp_.*$"}
    ],
    "transport": {"type": "console"}
}
client = OpenLineageClient(config=config)
```

</TabItem>
</Tabs>


## Tags

### Configuration

Custom tags can be added to jobs and runs, which are included in OpenLineage events as `TagsJobFacet` and `TagsRunFacet` respectively.
To enable this, configure the `tags` section of the configuration with separate dictionaries for `job` and `run` tags.

:::caution
User-supplied tags can override integration tags with the same key (case-insensitive).
:::

### Examples

<Tabs groupId="tags">
<TabItem value="env-vars" label="Environment Variables">

```sh
OPENLINEAGE__TAGS__JOB__ENVIRONMENT="PRODUCTION"  # Job tag
OPENLINEAGE__TAGS__JOB__PIPELINE="sales_monthly"  # Job tag
OPENLINEAGE__TAGS__RUN__adhoc="true"  # Run tag
```

</TabItem>
<TabItem value="env-var-single" label="Single Environment Variable">

```sh
OPENLINEAGE__TAGS='{"job": {"ENVIRONMENT": "PRODUCTION", "PIPELINE": "sales_monthly"}, "run": {"adhoc": "true"}}'
```

</TabItem>
<TabItem value="yaml" label="YAML Config">

```yaml
tags:
  job:
    ENVIRONMENT: PRODUCTION
    PIPELINE: sales_monthly
  run:
    adhoc: "true"
```

</TabItem>
<TabItem value="python" label="Python Code">

```python
from openlineage.client import OpenLineageClient

config = {
    "tags": {
        "job": {
            "ENVIRONMENT": "PRODUCTION",
            "PIPELINE": "sales_monthly"
        },
        "run": {
            "adhoc": "true"
        }
    },
    "transport": {"type": "console"}
}
client = OpenLineageClient(config=config)
```

</TabItem>
</Tabs>

