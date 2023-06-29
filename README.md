<div align="center">
  <img src="./doc/openlineage-logo.png" width="375px" />
  <a href="https://lfaidata.foundation/projects">
      <img src="./doc/lfaidata-project-badge-sandbox-black.png" width="115px" />
  </a>
</div>

## Badges

[![CircleCI](https://circleci.com/gh/OpenLineage/OpenLineage/tree/main.svg?style=shield)](https://circleci.com/gh/OpenLineage/OpenLineage/tree/main)
[![status](https://img.shields.io/badge/status-active-brightgreen.svg)](#status)
[![Slack](https://img.shields.io/badge/slack-chat-blue.svg)](http://bit.ly/OpenLineageSlack)
[![license](https://img.shields.io/badge/license-Apache_2.0-blue.svg)](https://github.com/OpenLineage/OpenLineage/blob/main/LICENSE)
[![maven](https://img.shields.io/maven-central/v/io.openlineage/openlineage-java.svg)](https://search.maven.org/search?q=g:io.openlineage)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4888/badge)](https://bestpractices.coreinfrastructure.org/projects/4888)

## Overview
OpenLineage is an Open standard for metadata and lineage collection designed to instrument jobs as they are running.
It defines a generic model of run, job, and dataset entities identified using consistent naming strategies.
The core lineage model is extensible by defining specific facets to enrich those entities.

## Status

OpenLineage is an [LF AI & Data Foundation](https://lfaidata.foundation/projects/openlineage) incubation project under active development, and we'd love your help!

## Problem

### Before

- Duplication of effort: each project has to instrument all jobs
- Integrations are external and can break with new versions

![Before OpenLineage](doc/before-ol.svg)

### With OpenLineage

- Effort of integration is shared
- Integration can be pushed in each project: no need to play catch up

![With OpenLineage](doc/with-ol.svg)

## Scope
OpenLineage defines the metadata for running jobs and the corresponding events.
A configurable backend allows the user to choose what protocol to send the events to.
 ![Scope](doc/scope.svg)

## Core model

 ![Model](doc/datamodel.svg)

 A facet is an atomic piece of metadata attached to one of the core entities.
 See the spec for more details.

## Spec
The [specification](spec/OpenLineage.md) is defined using OpenAPI and allows extension through custom facets.

## Integrations

The OpenLineage repository contains integrations with several systems.

- [Apache Spark](https://github.com/OpenLineage/OpenLineage/tree/main/integration/spark)
- [Apache Airflow](https://github.com/OpenLineage/OpenLineage/tree/main/integration/airflow)
- [Dagster](https://github.com/OpenLineage/OpenLineage/tree/main/integration/dagster)
- [dbt](https://github.com/OpenLineage/OpenLineage/tree/main/integration/dbt)
- [Flink](https://github.com/OpenLineage/OpenLineage/tree/main/integration/flink)

## Related projects
- [Marquez](https://marquezproject.ai/): Marquez is an [LF AI & DATA](https://lfaidata.foundation/) project to collect, aggregate, and visualize a data ecosystem's metadata. It is the reference implementation of the OpenLineage API.
  - [OpenLineage collection implementation](https://github.com/MarquezProject/marquez/blob/main/api/src/main/java/marquez/api/OpenLineageResource.java)
- [Egeria](https://egeria.odpi.org/): Egeria offers open metadata and governance for enterprises - automatically capturing, managing and exchanging metadata between tools and platforms, no matter the vendor.

## Community
- Website: [openlineage.io](http://openlineage.io)
- Slack: [OpenLineage.slack.com](http://bit.ly/OLslack)
  - Not a member? Join [here](bit.ly/OpenLineageSlack).
- Twitter: [@OpenLineage](https://twitter.com/OpenLineage)
- Mailing list: [openlineage-tsc](https://lists.lfaidata.foundation/g/openlineage-tsc)
- Wiki: [OpenLineage+Home](https://wiki.lfaidata.foundation/display/OpenLineage/OpenLineage+Home)
- LinkedIn: [13927795](https://www.linkedin.com/groups/13927795/)
- YouTube: [channel](https://www.youtube.com/channel/UCRMLy4AaSw_ka-gNV9nl7VQ)
- Mastodon: [@openlineage@fostodon.org](openlineage@fosstodon.org)

## Talks
- [Data+AI Summit June 2023. Cross-Platform Data Lineage with OpenLineage](https://www.databricks.com/dataaisummit/session/cross-platform-data-lineage-openlineage/)
- [Berlin Buzzwords June 2023. Column-Level Lineage is Coming to the Rescue](https://youtu.be/xFVSZCCbZlY)
- [Berlin Buzzwords June 2022. Cross-Platform Data Lineage with OpenLineage](https://www.youtube.com/watch?v=pLBVGIPuwEo)
- [Berlin Buzzwords June 2021. Observability for Data Pipelines with OpenLineage](https://2021.berlinbuzzwords.de/member/julien-le-dem)
- [Data Driven NYC February 2021. Data Observability and Pipelines: OpenLineage and Marquez](https://mattturck.com/datakin/)
- [Big Data Technology Warsaw Summit February 2021. Data lineage and Observability with Marquez and OpenLineage](https://bigdatatechwarsaw.eu/edition-2021/)
- [Metadata Day 2020. OpenLineage Lightning Talk](https://www.youtube.com/watch?v=anlV5Er_BpM)
- [Open Core Summit 2020. Observability for Data Pipelines: OpenLineage Project Launch](https://www.coss.community/coss/ocs-2020-breakout-julien-le-dem-3eh4)

## Contributing

See [CONTRIBUTING.md](https://github.com/OpenLineage/OpenLineage/blob/main/CONTRIBUTING.md) for more details about how to contribute.

## Report a Vulnerability

If you discover a vulnerability in the project, please [open an issue](https://github.com/OpenLineage/OpenLineage/issues/new/choose) and attach the "security" label.

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2023 contributors to the OpenLineage project