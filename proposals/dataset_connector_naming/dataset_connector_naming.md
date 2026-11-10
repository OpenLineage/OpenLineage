---
Author: Roman Savchenko
Created: November 10, 2026
Issue: Pending
---

# Dataset naming for additional connectors

## Purpose

Define consistent dataset namespaces and names for connectors missing from the naming documentation.
Producers and catalogs need the same identifiers to connect lineage for the same source object.

This proposal covers 27 candidate mapping groups and a SharePoint list extension. The formats below are candidates
for discussion; account, tenant, database, and resource scope must distinguish datasets before a convention is adopted.
Existing naming conventions remain unchanged.

## Proposed implementation

Agree on each connector's source identity and object identity, then add accepted mappings to the
[dataset naming conventions](../../website/docs/spec/naming.md).
Coordinate Java and Python naming helpers and the integrations that emit these identifiers as subsequent changes.
No new facet or JSON Schema change is proposed.

| Data Store | Namespace | Name |
| --- | --- | --- |
| MongoDB, MongoDB Atlas | `mongodb://{host}:{port}` | `{database}.{collection}` |
| MariaDB, MariaDB on RDS | `mysql://{host}:{port}` | `{database}.{table}` |
| Greenplum | `greenplum://{host}:{port}` | `{database}.{schema}.{table}` |
| CockroachDB | `cockroachdb://{host}:{port}` | `{database}.{schema}.{table}` |
| AlloyDB | `alloydb://{host}:{port}` | `{database}.{schema}.{table}` |
| SAP HANA | `hana://{host}:{port}` | `{database}.{schema}.{table}` |
| Impala | `impala://{host}:{port}` | `{database}.{table}` |
| Denodo | `denodo://{host}:{port}` | `{database}.{view}` |
| Splunk | `splunk://{host}:{port}` | `{index}` |
| Redis | `redis://{host}:{port}` | `{key}` |
| Exasol | `exasol://{host}` | `{schema}.{table}` |
| DynamoDB | `dynamodb://dynamodb.{awsRegion}.amazonaws.com` | `{table}` |
| Salesforce | `salesforce://{servername}` | `{object}` |
| Shopify | `shopify://{shop host}` | `{resource}` |
| Jira | `jira://{host from url}` | `{project}.{resource}` |
| Zendesk | `zendesk://{subdomain}` | `{resource}` |
| Freshdesk | `freshdesk://{domain}` | `{resource}` |
| QuickBase | `quickbase://{domain}` | `{resource}` |
| ServiceNow | `servicenow://{instance}` | `{table}` |
| Workday | `workday://{tenant}` | `{resource}` |
| Magento | `magento://{host from url}` | `{resource}` |
| Tableau | `tableau://{host}` | `{site}.{resource}` |
| Airtable | `airtable://{baseid}` | `{tablename}` |
| Qlik Cloud | `qlik://{host from baseUrl}` | `{resource}` |
| REST API | `rest://{host from base_URI}` | `{path}` |
| SFTP | `sftp://{host}:{port}` | `{path}` |
| TFS (ADS) | `tfs://{storage}` | `{path}` |

MariaDB proposes reusing the MySQL convention. MongoDB Atlas and MariaDB on RDS share their product family's entry.
SharePoint lists would reuse the existing SharePoint site namespace; list ID versus display name requires agreement.
Authentication variants do not define separate dataset identities.

## Decisions for discussion

| Connector or family | Question to resolve |
| --- | --- |
| DynamoDB | Include account scope: region and table alone can collide across accounts. Define AWS partitions and local endpoint handling. |
| Redis | Include logical database scope and decide whether a dataset represents a key, key pattern, or collection. |
| MongoDB and MongoDB Atlas | Define stable cluster identity, SRV handling, seed lists, and ports. Do not standardize shard-host suffix trimming without validating it. |
| Greenplum, CockroachDB, and AlloyDB | Choose product-specific schemes versus PostgreSQL naming, considering existing producers and catalogs. |
| Exasol | Decide whether the port is required to distinguish instances sharing a host. |
| Salesforce and Workday | Confirm organization or tenant identity across login endpoints, aliases, and environments. |
| Jira, QuickBase, Tableau, Airtable, and other hosted services | Define stable resource IDs and all required project, application, site, base, and tenant scope. |
| REST API | Define identity-bearing path and query components without including credentials; host and path may not distinguish tenants. |
| SFTP | Define absolute versus home-relative paths and port defaults. |
| TFS (ADS) | Establish a public product definition, storage identity, and path format before adopting this entry. |
| SharePoint lists | Choose list IDs versus names and preserve the complete site identity. |
| All new entries | Define default ports, case, escaping, embedded separators, and required identity fields. |

Lowercase DNS hosts while preserving source object and path case. Parse identity-bearing site and tenant components
before removing URL paths. Exclude credentials, authentication fingerprints, and unrelated connection options.
Do not use credentials or their hashes as source identity. Agree on normalization per connector rather than changing
existing conventions.

## Examples

These synthetic examples illustrate candidate formats; they do not resolve the decisions above.

| Source | Namespace | Name |
| --- | --- | --- |
| MariaDB | `mysql://db.example.com:3306` | `analytics.orders` |
| Greenplum | `greenplum://db.example.com:5432` | `analytics.public.orders` |
| MongoDB | `mongodb://db.example.com:27017` | `analytics.orders` |
| SFTP | `sftp://files.example.com:22` | `exports/orders.csv` |

## Deferred mappings

HubSpot, Mailchimp, Trello, Authorize.Net, SendGrid, Gemini, and Power BI need further identification requirements
before formats can be proposed. A connector must supply a usable account, tenant, workspace, or other instance
identifier independent of credentials. Gemini also needs an unambiguous product definition.

SharePoint authentication variants can reuse existing naming when site identity is available.
Parameterized datasets, custom SQL, and producer-managed storage resolution are outside this proposal.

## Compatibility and validation

Adding conventions must not silently rename existing datasets. Compare proposed identifiers with existing
producer and catalog behavior, and document migration where producers already use another format.

Before adopting a mapping, verify its canonical identifiers against vendor documentation and producer/catalog examples.
When implementing naming helpers, add equivalent Java and Python tests covering case, ports, quoting, URL parsing,
invalid input, and collisions across accounts, tenants, databases, and instances. Verify that credentials are excluded
while identity-bearing scope is preserved. Run repository hooks and the relevant client and integration checks.

This change is a design proposal only. Existing naming documentation, schemas, clients, and integrations are unchanged.

## Review process

Use the proposal issue template's Purpose and Proposed implementation sections to start discussion.
The issue number is pending: replace the Issue header and move this directory to `proposals/{issue-number}/`
once the issue exists. This draft does not imply community agreement.

Follow the [proposal review process](../336/PROPOSALS.md): establish agreement on the general direction, request
review from at least one committer, and submit accepted naming and implementation changes after design approval.

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2026 contributors to the OpenLineage project
