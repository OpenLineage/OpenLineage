---
sidebar_position: 3
title: Test Suite Workflows
---

# Test Suite Workflows Overview

The test suite contains three workflows for different use cases. Most of the steps in the workflows are similar - each workflow:
- Checks which component tests should be run
- Runs the tests to produce test reports  
- Collects the tests and checks for new failures

However, each workflow has a different purpose and scope:

|                             | **New Release**                                                                 | **Spec Update**                                                            | **Test Suite PR**                                       |
|:----------------------------|:--------------------------------------------------------------------------------|:---------------------------------------------------------------------------|:--------------------------------------------------------|
| **Goal**                    | Update compatibility data                                                       | Notify OpenLineage developers about potential backward compatibility issues | Check if changes in the PR are not causing new failures |
| **Trigger**                 | Periodic run with checks for new releases of components or OpenLineage         | Periodic run with checks for updates of spec in OpenLineage main branch   | PR to Test Suite repository                             |
| **Tested Components Scope** | Producers and Consumers                                                         | Producers and Consumer Input Events                                        | Producers, Consumers and Consumer Input Events          |
| **Component Selection**     | Components with new releases or all components in case of new OpenLineage release | All Producers and Consumer Input Events                                    | Producers, Consumers and Consumer Input Events          |
| **OpenLineage Versions**    | Release Versions                                                                | Latest snapshot version from main branch                                   | Release Versions                                        |
| **Additional Steps**        | Notify about new failures, update test report, update compatibility information | Notify about new failures                                                  | -                                                       |


