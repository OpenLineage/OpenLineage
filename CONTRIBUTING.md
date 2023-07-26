# Contributing to OpenLineage

This project welcomes contributors from any organization or background, provided they are
willing to follow the simple processes outlined below, as well as adhere to the 
[Code of Conduct](CODE_OF_CONDUCT.md).

## Joining the community

The community collaborates primarily through  `GitHub` and the instance messaging tool, `Slack`.
There is also a mailing list.
See how to join [here](https://github.com/OpenLineage/OpenLineage#community)

## Reporting an Issue

Please use the [issues][issues] section of the OpenLineage repository and search for a similar problem. If you don't find it, submit your bug, question, proposal or feature request.

Use tags to indicate parts of the OpenLineage that your issue relates to.
For example, in the case of bugs, please provide steps to reproduce it and tag your issue with `bug` and integration that has that bug, for example `integration/spark`.


## Contributing to the project

### Creating Pull Requests
Before sending a Pull Request with significant changes, please use the [issue tracker][issues] to discuss the potential improvements you want to make.

OpenLineage uses [GitHub's fork and pull model](https://help.github.com/articles/about-collaborative-development-models/)
to create a contribution.

Make sure to [sign-off](https://github.com/OpenLineage/OpenLineage/blob/main/why-the-dco.md) your work to say that the contributor has the rights to make the contribution and
agrees with the [Developer Certificate of Origin (DCO)](why-the-dco.md).

To ensure your pull request is accepted, follow these guidelines:

* All changes should be accompanied by tests
* Do your best to have a [well-formed commit message](https://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html) for your change
* Do your best to have a [well-formed](https://frontside.com/blog/2020-7-reasons-for-good-pull-request-descriptions) pull request description for your change
* [Keep diffs small](https://kurtisnusbaum.medium.com/stacked-diffs-keeping-phabricator-diffs-small-d9964f4dcfa6) and self-contained
* If your change fixes a bug, please [link the issue](https://help.github.com/articles/closing-issues-using-keywords) in your pull request description
* Your pull request title should be of the form `component: name`, where `component` is the part of openlineage repo that your PR changes. For example: `flink: add Iceberg source visitor`
* Use tags to indicate parts of the repository that your PR refers to

### Branching

* Use a _group_ at the beginning of your branch names:

  ```
  feature  Add or expand a feature
  bug      Fix a bug
  ```

  _For example_:

  ```
  feature/my-cool-new-feature
  bug/my-bug-fix
  bug/my-other-bug-fix
  ```

* Choose _short_ and _descriptive_ branch names
* Use dashes (`-`) to separate _words_ in branch names
* Use _lowercase_ in branch names

## Proposing changes

Create an issue and tag it as `proposal`.

In the description provide the following sections:
 - Purpose (Why?): What is the use case this is for. 
 - Proposed implementation (How?): Quick description of how do you propose to implement it. Are you proposing a new facet?

This can be just a couple paragraphs to start with.

Proposals that change OpenLineage specifications should be tagged as `spec`.
Small changes to the spec, like adding a facet, only require opening an issue describing the new facet.
Larger changes to the spec, changes to the core spec or new integrations require a longer form proposal following [this process](https://github.com/OpenLineage/OpenLineage/blob/main/proposals/336/PROPOSALS.md)

## New Integrations
New integrations should be added under the [./integrations](/integrations) folder. Each module
should have its own build configuration (e.g., `build.gradle` for a Gradle project, `setup.py` for 
python, etc.) with appropriate unit tests and integration tests (when possible).

Adding a new integration requires updating the CI build configuration with a new workflow. Job
definitions, orbs, parameters, etc. shoudl be added to the
[.circleci/continue_config.yml](`continue_config.yml`) file. Workflow definition files are added to
the [.circleci/workflows](.circleci/workflows) directory. Each workflow file adheres to the CircleCI
config.yml schema, including only the workflows subschema (see
[https://circleci.com/docs/2.0/configuration-reference/#workflows](the CircleCI docs) for the schema
specification). Each workflow must include a `workflow_complete` job that `requires` each terminal
required step in the workflow (e.g., you might depend on `run-feature-integration-tests` as the
final step in the workflow). Job names must be unique across all workflows, as ultimately the
workflows are merged into a single config file. See existing workflows for examples.

## First-Time Contributors

If this is your first contribution to open source, you can [follow this tutorial][contributiontutorial] or check out [this video series][contributionvideos] to learn about the contribution workflow with GitHub.

Look for tickets labeled ['good first issue'][goodfirstissues] and ['help wanted'][helpwantedissues]. These are a great starting point if you want to contribute. Don't hesitate to ask questions about the issue if you are not sure about the strategy to follow.


[issues]: https://github.com/OpenLineage/OpenLineage/issues
[contributiontutorial]: https://github.com/firstcontributions/first-contributions#first-contributions
[contributionvideos]: https://egghead.io/courses/how-to-contribute-to-an-open-source-project-on-github
[goodfirstissues]: https://github.com/OpenLineage/OpenLineage/labels/good%20first%20issue
[helpwantedissues]: https://github.com/OpenLineage/OpenLineage/labels/help%20wanted

## Running pre-commit hooks

Before submitting your pull request, make sure to set up and run pre-commit hooks to ensure code quality and consistency. [Pre-commit](pre-commit.com) hooks are automated checks that run before each commit is made. These checks include code formatting, linting and JSON Schema specification validations. To set up the pre-commit hooks for this project, follow these steps:

* Install pre-commit: If you haven't already, install pre-commit on your local machine by running `pip install pre-commit` in your virtual environment.

* Set up hooks: Once pre-commit is installed, navigate to the project's root directory and execute `pre-commit install`. This command will set up the necessary hooks in your local repository.

* Run pre-commit: Now, every time you attempt to make a commit, the pre-commit hooks will automatically run on the staged files. If any issues are detected, the commit process will be halted, allowing you to address the problems before making the commit. You can also run `pre-commit run --all-files` to manually trigger the hooks for all files in the repository.


## Triggering CI runs from forks (committers)

CI runs on forks are disabled due to the possibility of access by external services via CI run. 
Once a contributor decides a PR is ready to be checked, they can use [this script](https://github.com/jklukas/git-push-fork-to-upstream-branch)
to trigger a CI run on a separate branch with the same commit ID. This will update the CI status of a PR.

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2023 contributors to the OpenLineage project
