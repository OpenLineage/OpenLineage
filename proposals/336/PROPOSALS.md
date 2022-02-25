<!-- SPDX-License-Identifier: Apache-2.0 -->

---
Author: Julien Le Dem
Created: November 3rd
Issue: https://github.com/OpenLineage/OpenLineage/issues/336
---

# Purpose

Define a design review for proposals that warant more discussion and design than fits in an issue description

# Implementation

- Proposal design documents live under https://github.com/OpenLineage/OpenLineage/tree/main/proposals
- one folder per design (includes images and the proposal document)
- the proposal is keyed by the github issue id: "/proposals/{github issue id}/{short description}.md"
- each doc has a header with:
 - Author: name of the main author (or authors)
 - Created: the date this was created
 - Issue: the issue tracking this design

# lifecycle:
 
- open an issue using the PROPOSAL template.
- if the discussion warrants it and the general direction is aligned with the project, create a design doc in the /proposals folder by submitting a Pull Request.
- the community can voice it's support in the issue or provide feedback on the PR.
- The PR should be assigned to at least one committer for review.
- once the PR is approved, the design is approved (and in the main branch). It can be prioritized for implementation.
- [a] separate[s] ticket[s] can be created for the implementation (attaching them to a milestone allows tracking progress of the implementation)

