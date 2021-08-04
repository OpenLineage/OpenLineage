# OpenLineage-python

To install from source, run:

```bash
$ python setup.py install
```

## Configuration

OpenLineage client depends on environment variables:

* `OPENLINEAGE_URL` - point to service which will consume OpenLineage events
* `OPENLINEAGE_API_KEY` - set if consumer of OpenLineage events requires `Bearer` authentication key

`OPENLINEAGE_URL` and `OPENLINEAGE_API_KEY` can also be set up manually when creating client instance.
