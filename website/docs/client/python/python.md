---
sidebar_position: 5
title: Python
---

## Overview

The Python client is the basis of existing OpenLineage integrations such as Airflow and dbt.

The client enables the creation of lineage metadata events with Python code. 
The core data structures currently offered by the client are the `RunEvent`, `RunState`, `Run`, `Job`, `Dataset`, 
and `Transport` classes. These either configure or collect data for the emission of lineage events.

You can use the client to create your own custom integrations.

## Installation

Download the package using `pip` with 
```bash
pip install openlineage-python
```

To install the package from source, use
```bash
python -m pip install .
```

### Optional Dependencies

The Python client supports optional dependencies for enhanced functionality:

#### Remote Filesystem Support
For file transport with remote storage backends (S3, GCS, Azure, etc.):
```bash
pip install openlineage-python[fsspec]
```

#### Kafka Support
For Kafka transport:
```bash
pip install openlineage-python[kafka]
```

#### MSK IAM Support
For AWS MSK with IAM authentication:
```bash
pip install openlineage-python[msk-iam]
```

#### DataZone Support
For AWS DataZone integration:
```bash
pip install openlineage-python[datazone]
```

#### All Optional Dependencies
To install all optional dependencies:
```bash
pip install openlineage-python[fsspec,kafka,msk-iam,datazone]
```
