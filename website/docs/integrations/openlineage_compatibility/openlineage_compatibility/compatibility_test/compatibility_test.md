---
sidebar_position: 1
title: Compatibility Tests
---

## Compatibility Tests

The [Compatibility Tests](https://github.com/OpenLineage/compatibility-tests/) is a comprehensive test suite created to improve visibility and standardize the validation of OpenLineage compatibility with different components.

It consists of a GitHub repository with GitHub Actions workflows that continuously check compatibility between different versions of OpenLineage and various versions of producers or consumers. The results are interpreted and visualized as compatibility tables, which are presented in the [OpenLineage Compatibility](..) documentation.

The checks are performed by running syntactic and semantic validations on producers and consumers:
- **For producers**: We define test scenarios that generate OpenLineage events, which we validate for compliance with expected structure (syntax) and values in event fields (semantics)
- **For consumers**: We send valid OpenLineage events and verify they can be ingested properly (syntax) and produce the desired change in consumer state (semantics)

## Motivations

The OpenLineage community lacked a formalized way of determining whether components are compliant with the standard. Community members had to look up support information on vendor sites or documentation, often finding inconsistent or outdated information.

We wanted to create a centralized source of compliance information with standardized validation methods. Additionally, we aimed to provide an easy way for community members to verify their components' compatibility with OpenLineage standards.

## Assumptions

While creating the test suite, we focused on its usefulness to the community in several key aspects:

1. **Simple representation**: Test results should be presented in a clear, understandable format
2. **Easy contributions**: Making contributions should be as straightforward as possible
   - Each component with its test scenarios should have analogous structure and output
   - Each component should be independent of other components
   - Validation mechanisms should be generic and reusable
3. **Local execution**: Validation mechanisms should be runnable outside our workflows - the workflow should execute separately defined modules that can be run locally
4. **Comprehensive testing**: Tests should validate both syntactic and semantic compliance
5. **Documentation**: The test suite should be well documented
   - Producer scenarios should contain descriptions of operations, datasets, and facets
   - Consumer scenarios should describe expected state changes after consuming events
   - Each consumer should provide mapping between OpenLineage event entities and its own data model