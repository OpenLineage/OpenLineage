### Problem

👋 Thanks for opening a pull request! Please include a brief summary of the problem your change is trying to solve, or bug fix. If your change fixes a bug or you'd like to provide context on why you're making the change, please [link the issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue) as follows:

Closes: #ISSUE-NUMBER

### Solution

Please describe your change as it relates to the problem, or bug fix, as well as any dependencies. If your change requires a schema change, please describe the schema modifcation(s) and whether it's a _backwards-incompatible_ or _backwards-compatible_ change, then select one of the following (_if relevant_):

> **Note:** All schema changes require discussion. Please [link the issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue) for context.

- [ ] Your change modifies the [core](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json) OpenLineage model
- [ ] Your change modifies one or more OpenLineage [facets](https://github.com/OpenLineage/OpenLineage/tree/main/spec/facets)

If you're contributing a new integration, please specify the scope of the integration and how/where it has been tested (e.g., Apache Spark integration supports `S3` and `GCS` filesystem operations, tested with AWS EMR).

### Checklist

- [ ] You've [sign-off](https://github.com/OpenLineage/OpenLineage/blob/main/why-the-dco.md) your work
- [ ] Your pull request title follows our [guidelines](https://github.com/OpenLineage/OpenLineage/blob/main/CONTRIBUTING.md#creating-pull-requests)
- [ ] Your changes are accompanied by tests (_if relevant_)
- [ ] Your change contains a [small diff](https://kurtisnusbaum.medium.com/stacked-diffs-keeping-phabricator-diffs-small-d9964f4dcfa6) and is self-contained
- [ ] You've updated any relevant documentation (_if relevant_)
- [ ] You've updated the [`CHANGELOG.md`](https://github.com/OpenLineage/OpenLineage/blob/main/CHANGELOG.md) with details about your change under the "Unreleased" section (_if relevant, depending on the change, this may not be necessary_)
- [ ] You've versioned the core OpenLineage model or facets according to [SchemaVer](https://docs.snowplowanalytics.com/docs/pipeline-components-and-applications/iglu/common-architecture/schemaver) (_if relevant_)