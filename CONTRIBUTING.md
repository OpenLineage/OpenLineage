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

In the case of bugs, please provide steps to reproduce it and tag your issue with "bug"

## Contributing to the project

### Creating Pull Requests
Before sending a Pull Request with significant changes, please use the [issue tracker][issues] to discuss the potential improvements you want to make.

OpenLineage uses [GitHub's fork and pull model](https://help.github.com/articles/about-collaborative-development-models/)
to create a contribution.

Make sure to [sign-off](https://github.com/OpenLineage/OpenLineage/blob/main/why-the-dco.md) your work to say that the contributor has the rights to make the contribution and
agrees with the [Developer Certificate of Origin (DCO)](why-the-dco.md)

To ensure your pull request is accepted, follow these guidelines:

* All changes should be accompanied by tests
* Do your best to have a [well-formed commit message](https://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html) for your change
* Do your best to have a [well-formed](https://frontside.com/blog/2020-7-reasons-for-good-pull-request-descriptions) pull request description for your change
* [Keep diffs small](https://kurtisnusbaum.medium.com/stacked-diffs-keeping-phabricator-diffs-small-d9964f4dcfa6) and self-contained
* If your change fixes a bug, please [link the issue](https://help.github.com/articles/closing-issues-using-keywords) in your pull request description
* Your pull request title should be of the form `[CATEGORY][COMPONENT] Your title`, where `CATEGORY` is either `PROPOSAL`, or `INTEGRATION`,
  and `COMPONENT` is one of `AIRFLOW`, `DBT`, `SPARK`, etc.

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

Create an issue and prefix the description with "[PROPOSAL]"

In the description provide the following sections:
 - Purpose (Why?): What is the use case this is for. 
 - Proposed implementation (How?): Quick description of how do you propose to implement it. Are you proposing a new facet?

This can be just a couple paragraphs to start with.

## First-Time Contributors

If this is your first contribution to open source, you can [follow this tutorial][contributiontutorial] or check [this video series][contributionvideos] to learn about the contribution workflow with GitHub.

Look tickets labeled ['good first issue'][goodfirstissues] and ['help wanted'][helpwantedissues]. These are a great starting point if you want to contribute. Don't hesitate to ask questions about the issue if you are not sure about the strategy to follow.


[issues]: https://github.com/OpenLineage/OpenLineage/issues
[contributiontutorial]: https://github.com/firstcontributions/first-contributions#first-contributions
[contributionvideos]: https://egghead.io/courses/how-to-contribute-to-an-open-source-project-on-github
[goodfirstissues]: https://github.com/OpenLineage/OpenLineage/labels/good%20first%20issue
[helpwantedissues]: https://github.com/OpenLineage/OpenLineage/labels/help%20wanted

## Triggering CI runs from forks (committers)

CI runs on forks are disabled due to possibility to access external services via CI run. 
Once contributor decides PR is ready to be checked, they can use [this script](https://github.com/jklukas/git-push-fork-to-upstream-branch)
to trigger CI run on separate branch with same commit ID. This will update CI status of a PR.