# OpenLineage-python

To install from source, run:

```bash
$ python setup.py install
```

## Configuration

OpenLineage client depends on `Transport` abstraction, which facilitates
sending OpenLineage events over various protocols, like `http` or `kafka`.

You can choose between transports by setting `OPENLINEAGE_TRANSPORT` environment variable. 
If the variable is not set, `http` transport is assumed.
The transports themselves may depend on additional environment variables.

Currently, only `http` transport is build in and detected via `OPENLINEAGE_TRANSPORT`.

Client can be also constructed as a standard Python class.

### HTTP transport
This transport depends on environment variables:

* `OPENLINEAGE_URL` - point to service which will consume OpenLineage events
* `OPENLINEAGE_API_KEY` - set if consumer of OpenLineage events requires `Bearer` authentication key
