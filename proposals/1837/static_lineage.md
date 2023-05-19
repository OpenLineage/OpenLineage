---
Author: "Julien Le Dem (with contributions from Maciej Obuchowski, Benji Lampel and Ross Turk)"
Created: May 9th 2023
Issue: https://github.com/OpenLineage/OpenLineage/issues/1837
---

# Purpose

Today OpenLineage captures metadata mainly in the context of a run that is actively accessing and modifying/producing data. The metadata collected covers both dynamic aspects that are specific to the run (how long it took, how many rows were produced, ...) and static metadata that does not change every time the job runs (inputs, outputs, dataset schema, code version, ...).

We're referring to those two types of metadata as dynamic and static.

We want to collect that static metadata for Datasets and Jobs outside of the context of a run through OpenLineage.
Examples:

defining dataset owners
marking a dataset as containing personally identifying information.

# Implementation

To collect OpenLineage `Dataset` and `Job` metadata that does not exist in a context of a `Run`, 
we'll be adding two new events in addition to the existing Run event: Dataset and Job events. 

## Summary of current events
Currently the OpenLineage spec defines run events which describe how a job is writing in output datasets based on data it’s reading in input datasets. 
This describes operational lineage: It captures metadata as the transformation is happening and enables precise metadata of that transformation and effects on datasets.

Those events capture metadata related to the run itself and metadata that is more static and related to the job and datasets.

On the job side we capture:
- job facets: describe the current state of the job. example: the current version of the code (git sha, …).
- run facets: describe the current run. Run is an instance - execution of a job, and always has a job. Examples: the query profile; how many cluster milliseconds where consumed

On the dataset side we capture:
- dataset facets: describe the current state of the dataset. example: the schema 
- input and output facets: describe metadata of the transformation happening in the current run. examples: number of rows produced; specific iceberg version consumed.

In each case we capture static metadata (job and dataset facets) and metadata related to the run itself (run, input and output facets)

We currently don’t have a way to capture static metadata outside of runs.

## Objective
Develop a model or set of models to emit Dataset and Job metadata outside of a RunEvent. 
This will be considered complete when:
- Dataset events emit from OpenLineage absent a RunEvent
- Job events emit from OpenLineage absent a RunEvent
- A plan for consumers to ingest these new events is implemented

## Use cases
### Bootstrapping
A user may also want to bootstrap their lineage graph with jobs that have not run yet, or ensure that all possible paths of a DAG are represented (in the case of audits, for example), even if they have not ever occurred.
### Dataset ownership change
A dataset can change owners through means that OpenLineage would not currently process, for instance via a database manager changing the owner of a table.
### Consume facets from external systems
An external system that maintains data quality, object tags, or documentation can emit dataset metadata to OpenLineage without requiring creation of a job/run 
### Create dataset symlinks more easily
In cases where integrations report on the same dataset differently (i.e., FQDN versus hostname, including region vs. not, differing query params) and a symlink needs to be created, this can be done without synthesizing a job/run. 
### Record datasets before access
Is this just a form of bootstrapping?
### Dataset connections
Ability to connect 2 dataset entities without additional information about the run that created that connection

## Proposed Model
### Extension
We propose to expand this by enabling describing static lineage metadata which is known independently of any access to the datasets (outside of runs).
In this context we allow three types of events:
- run events: the existing OpenLineage run events
  - run: 
    - run id
    - run facets
  - job: 
    - job id
    - job facets
  - for each input: 
    - dataset id
    - dataset facets
    - input facets
  - for each output: 
    - dataset id
    - dataset facets
    - output facets
- Dataset events: describe static dataset attribute (schema, owner, ...)
  - dataset id
  - dataset facets
- Job events: describe static job attributes (current version, …)
  - job id
  - job facets
  - for each input: 
    - dataset id
    - dataset facets (there may not be any in this context)
  - for each output: 
    - dataset id
    - dataset facets (there may not be any in this context)

## Semantics of facets across jobs and dataset version
In Marquez, jobs and datasets are versioned as run events are received. 
A run id is connected to:
- the version of a job that it ran (what version of the code, …)
- the version of a dataset it read from
- the version of a dataset it produced
Facets are attached to the particular version created by a run:
- run and input/output facets are connected to the run they were attached to in the run events for that run.
- job and dataset facets are connected respectively to the job or dataset versions created when the run events were consumed.

Now, we are adding Job and Dataset events that will add facets at a point in time. 

The expected semantics is that job and dataset facets apply to that entity for that moment on until they are replaced by a new version of the same facet. For example, sending a Dataset event with an ownership facet on a dataset, replaces any previously defined ownership facet and applies to all versions moving forward until replaced by a new one.

In contrast, run, input and output facets only apply to the run they are attached to and the specific Job or Dataset versions.


----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2023 contributors to the OpenLineage project
