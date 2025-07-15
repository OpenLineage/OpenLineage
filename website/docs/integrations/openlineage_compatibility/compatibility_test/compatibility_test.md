---
sidebar_position: 1
title: Compatibility Tests
---

## Compatibility Tests

The [Compatibility Tests](https://github.com/OpenLineage/compatibility-tests/) are a comprehensive test suite created to improve visibility and standardize the validation of OpenLineage compatibility with different components.

It consists of a GitHub repository with GitHub Actions workflows that continuously check compatibility between different versions of OpenLineage and various versions of producers or consumers. The results are interpreted and visualized as compatibility tables, which are presented in the [OpenLineage Compatibility](..) documentation.

The checks are performed by running syntactic and semantic validations on producers and consumers:
- **For producers**: We define test scenarios that generate OpenLineage events, which we validate for compliance with expected structure (syntax) and values in event fields (semantics)
- **For consumers**: We send valid OpenLineage events and verify they can be ingested properly (syntax) and produce the desired change in consumer state (semantics)

## Motivations

The OpenLineage community lacks a formalized way of determining whether components are compliant with the standard. 
Community members had to look up support information on vendor sites or documentation, often finding inconsistent or outdated information.

## Goals

There are three main groups in OpenLineage community, people who contribute to OpenLineage,
people who contribute to components compatible with OpenLineage and people who use OpenLineage with said software.
We wanted our test suite to provide information those people may want about OpenLineage.

For component contributors:
- continuously test if their components are compatible with multiple versions of OpenLineage on the level of:
  - integration - are there any issues when component is run with OpenLineage integration (producers)
  - syntax - do emitted events comply with OpenLineage standard (producer) or can be consumed without error (consumer)
  - semantics - do emitted events reflect the logic correctly (producer) or are they mapped into consumer entities in correct way (consumer)
- provide a way to validate their events by themselves

For OpenLineage contributors:
- continuously test if new or updated facets are backwards compatible
- have an early warning for issues in new releases of components integrations

For OpenLineage users:
- generate up to date and easily accessible information about how well OpenLineage is supported by various components.
- have examples of OpenLineage events produced by different components

## Assumptions

While creating the test suite, we focused on its usefulness to the community in several key aspects:

1. **Simple representation**: Test results should be presented in a clear, understandable format
2. **Easy contributions**: Making contributions should be as straightforward as possible
   - Each component with its test scenarios should have consistent structure and output
   - Each component should be independent of other components
   - Validation mechanisms should be generic and reusable
3. **Local execution**: Validation mechanisms should be runnable outside our workflows - the workflow should execute separately defined modules that can be run locally
4. **Comprehensive testing**: Tests should validate both syntactic and semantic compliance
5. **Documentation**: The test suite should be well documented
   - Producer scenarios should contain descriptions of operations, datasets, and facets
   - Consumer scenarios should describe expected state changes after consuming events
   - Each consumer should provide mapping between OpenLineage event entities and its own data model