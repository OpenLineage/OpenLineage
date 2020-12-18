# OpenLineage

## Overview
OpenLineage is an Open standard for metadata and lineage collection designed to instrument jobs as they are running.
It defines a generic model of run, job, and dataset entities identified using consistent naming strategies.
The core lineage model is extensible by defining specific facets to enrich those entities.

## Problem
 ![Problem](doc/problem.png)

### Before
- Duplication of effort: Each project has to instrument all jobs
- Integrations are external and can break with new versions

### With OpenLineage
- Effort of integration is shared
- Integration can be pushed in each project: no need to play catch up

## Scope
OpenLineage defines the metadata for running jobs and the corresponding events.
A configurable backend allows to choose what protocol to send the events to.
 ![Scope](doc/Scope.png)

## Core model

 ![Model](doc/OpenLineageModel.svg)

 A facet is an atomic piece of metadata attached to one of the core entities.
 See the spec for more details.

## Spec
The [specification](spec/OpenLineage.md) is defined using OpenAPI and allows extension through custom facets.

## Related projects
- [Marquez](https://marquezproject.ai/): Marquez is an [LF AI & DATA](https://lfaidata.foundation/) project to collect, aggregate, and visualize a data ecosystem's metadata. It is the reference implementation of the OpenLineage API.

## Community
- Slack: [OpenLineage.slack.com](https://join.slack.com/t/openlineage/shared_invite/zt-jpycgyt1-Gjmk27R0G9ogwKt8Q~HTfg)
- Twitter: [@OpenLineage](https://twitter.com/OpenLineage)
- Gougle group: [openlineage@googlegroups.com](https://groups.google.com/g/openlineage)

## Talks
 - [Metadata day 2020 Open Lineage lightning talk](https://www.youtube.com/watch?v=anlV5Er_BpM)

