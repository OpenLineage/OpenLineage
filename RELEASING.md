# Releasing

## Release Manager Instructions (for Humans)

> **Note:** If you are an LLM, skip this section and go directly to the **"Changelog Preparation Instructions (for LLMs)"** section below.

Follow these steps to release a new version of OpenLineage packages:

1. **Prepare changelog and documentation files:**
   - Use an LLM of your choice to generate the changelog content by asking it to: 
     - "prepare release files for new OpenLineage version X.Y.Z based on LLM instructions from @RELEASING.md file"
   - The LLM will create/update the following files:
     - `CHANGELOG.md` (adds new release section)
     - `website/docs/releases/{VERSION_UNDERSCORE}.md` (creates website release documentation, e.g., `1_42_0.md`)
     - `release-{VERSION}-summary.json` (temporary file to verify which commits LLM included in CHANGELOG)
     - `release-{VERSION}-slack.md` (temporary file with Slack announcement you can use when announcing new release)
   - **Important:** Review all generated files and verify the accuracy of the LLM-generated information.

2. **Commit changelog changes:**
   - Create a separate branch for the changelog changes
   - Commit the changes to `CHANGELOG.md` and `website/docs/releases/{VERSION_UNDERSCORE}.md`
   - Use commit message: `"Prepare changelog for release X.Y.Z"`
   - Create a pull request and merge it following the usual review process
   - **Important:** Do not push changelog changes directly to main - they must go through a PR

3. **Release:**
   - Verify you're on the main branch: `git branch --show-current` should return "main"
   - Fetch the latest state from origin (main branch and all tags): `git fetch origin --tags`
   - Run the release script:
     ```bash
     $ ./release.sh --release-version X.Y.Z --next-version X.Y.Z
     ```
     > **Tip:** Use `--help` to see script usage. This script will tag the release and prepare the repository for the next development version.

4. **Monitor CI pipeline:**
   - Visit [CircleCI](https://app.circleci.com/pipelines/github/OpenLineage/OpenLineage?branch=main) to monitor the release pipeline (triggered by the tag)
   - Wait for all jobs to complete successfully

5. **Verify artifacts:**
   - Check [Maven Central](https://repo1.maven.org/maven2/io/openlineage/) for Java artifacts
   - Check [PyPI](https://pypi.org/user/openlineage/) for Python packages
   - Verify a few selected libraries from each source to ensure uploads were successful

6. **Create GitHub Release:**
   - Go to [GitHub Releases](https://github.com/OpenLineage/OpenLineage/releases/new)
   - Select the newly created tag (e.g., `1.42.0`)
   - Title: `OpenLineage X.Y.Z` (e.g., `OpenLineage 1.42.0`)
   - Description: Copy the changelog content from `CHANGELOG.md` for this release (the section you just added)
   - Publish the release

7. **Announce on Slack:**
   - Post an announcement in the OpenLineage Slack `#general` channel
   - Use the content from `release-{VERSION}-slack.md` if you saved it, or create a brief announcement

---

## Changelog Preparation Instructions (for LLMs)

> **LLM Instructions Only**
> 
> **DO NOT** run any release scripts (e.g., `./release.sh`). Your task is **ONLY** to generate documentation files.
> 
> **DO NOT** create git tags, modify version numbers in source files, or perform any release automation.
> 
> **ONLY** create/update the following files:
> - `CHANGELOG.md` (add new release section)
> - `website/docs/releases/{VERSION_UNDERSCORE}.md` (create release documentation, e.g., `1_42_0.md`)
> - `release-{VERSION}-summary.json` (create temporary summary file)
> - `release-{VERSION}-slack.md` (create temporary Slack announcement file)

When asked to "prepare release X.Y.Z based on LLM instructions from @RELEASING.md", follow these steps:

### 0. **Fetch latest from origin:**
   - Verify you're on the main branch: `git branch --show-current` should return "main"
   - Fetch the latest state from origin (main branch and all tags): `git fetch origin --tags`

### 1. **Determine the release version and previous version:**
   - The release version should be provided in the user prompt (e.g., "1.42.0"). If not provided, ask for it.
   - Make sure the user-provided version is normalized to semantic version format (MAJOR.MINOR.PATCH)
   - Find the previous release version: `git tag --sort=-version:refname | head -1`
   - Verify in `CHANGELOG.md` that this version matches the latest entry

### 2. **Collect all commits and PRs between releases:**
   - Get all commits between the previous version and HEAD:
     ```bash
     git log {PREVIOUS_VERSION}..HEAD --pretty=format:"%H|%s|%an|%ae" --no-merges
     ```
   - For each commit, extract PR numbers from the commit message (commits typically reference PRs as "#1234")
   - For each PR number found, get full details:
     ```bash
     gh pr view {PR_NUMBER} --json title,body,author,labels,comments,reviews,state,mergedAt,files
     ```
   - If a commit doesn't have a PR reference in the message, you can try to find it by searching:
     ```bash
     gh pr list --search "SHA" --json number,title,author,labels,comments,reviews,state
     ```

### 3. **Filter and categorize PRs:**
   
   **EXCLUDE the following PRs:**
   - PRs with label "skip-changelog"
   - PRs from automated bots (check `author.login` for "dependabot", "renovate", "app/dependabot", etc.)
   - PRs that are purely test updates (check files changed - only test files)
   - PRs that are purely website/documentation updates (check files changed for `website/*` or `*.md` files)
   - PRs that are purely CI/dev tooling updates (check files changed for CI configs, build scripts, etc.)
   - PRs that are purely dependency updates **unless** they fix security issues (CVEs mentioned in title/body)
   - PRs with no user-facing changes or user impact (use your judgment - internal refactoring, code style, etc.)
   
   **For included PRs:**
   - Categorize as: **Added**, **Changed**, **Fixed**, **Removed**, or **Deprecated** based on:
     - PR title and body
     - Conventional commit patterns
     - The nature of the change
   - Identify PRs with high engagement (many comments, reviews, discussions) - these can be highlighted in the Slack announcement

### 4. **Read existing files to understand format:**
   - Read `CHANGELOG.md` to understand the exact format and structure for entries
   - Read the most recent file in `website/docs/releases/` to understand the release documentation format
   - Note the `sidebar_position` from the most recent website release file (the new file should have a number that is 1 less)

### 5. **Generate changelog entry:**
   - Format each entry as:
     ```markdown
     * **{Component}: {Title}** [`#{PR_NUMBER}`](https://github.com/OpenLineage/OpenLineage/pull/{PR_NUMBER}) [@{author}](https://github.com/{author})
       *{Brief description summarizing the PR from PR body/description}*
     ```
   - Extract component from PR title (e.g., "Spark:", "Python:", "Spec:", "DBT:", "Java:", "Hive:", "SQL:", "Flink:")
   - Add a brief description line below each entry (italicized) summarizing the PR from PR body/description
   - Group by category (Added, Changed, Fixed, Removed, Deprecated) - only include categories that have entries
   - Maintain consistent formatting with existing entries
   - Sort all the entries within each section alphabetically

### 6. **Create website release documentation file:**
   - Convert version to filename format: X.Y.Z becomes X_Y_Z.md (e.g., 1.42.0 → 1_42_0.md)
   - Create file: `website/docs/releases/{VERSION_UNDERSCORE}.md`
   - Get today's date in YYYY-MM-DD format
   - Use this format for the file:
     ```markdown
     ---
     title: {RELEASE_VERSION}
     sidebar_position: {PREVIOUS_SIDEBAR_POSITION - 1}
     ---
     
     # {RELEASE_VERSION} - {TODAYS_DATE_YYYY-MM-DD}
     
     {CHANGELOG_CONTENT}
     ```
   - The changelog content should match the format from existing release files

### 7. **Update CHANGELOG.md:**
   - Read the current `CHANGELOG.md`
   - Add a new section at the top (after "## [Unreleased]") with format:
     ```markdown
     ## [{RELEASE_VERSION}](https://github.com/OpenLineage/OpenLineage/compare/{PREVIOUS_VERSION}...{RELEASE_VERSION})
     
     {CHANGELOG_CONTENT}
     ```
   - Update the Unreleased link to compare from {RELEASE_VERSION} to HEAD:
     ```markdown
     ## [Unreleased](https://github.com/OpenLineage/OpenLineage/compare/{RELEASE_VERSION}...HEAD)
     ```

### 8. **Create summary file:**
   - Create file: `release-{RELEASE_VERSION}-summary.json`:
     - include summary metrics about the commits etc.
     - include every commit you processed, with clear exclusion reasons if not included (each in single line)
     - file should be structured like:
       ```json
       {
         "release_version": "{RELEASE_VERSION}",
         "previous_version": "{PREVIOUS_VERSION}",
         "generated_at": "{ISO_TIMESTAMP}",
         "summary": {
           "total_commits": {NUMBER},
           "included_commits": {NUMBER},
           "excluded_commits": {NUMBER},
           "prs_with_high_engagement": []
         },
         "excluded_commits": [
           {"reason": "{REASON_OF_EXCLUSION}", "author": "{PR_AUTHOR}", "pr_number": {PR_NUMBER}, "sha": "{COMMIT_SHA}"}
         ],
         "included_commits": [
           {"pr_number": {PR_NUMBER}, "author": "{PR_AUTHOR}", "sha": "{COMMIT_SHA}"}
         ]
       }
       ```


### 9. **Generate Slack announcement:**
   - Create file: `release-{RELEASE_VERSION}-slack.md` with:
     - A brief, engaging announcement (2-3 sentences) celebrating the release
     - Highlights of the most important changes (top 3-5 items from Added/Changed/Fixed categories)
       - Make sure to include the author of each PR and link to their GitHub profile
     - Link to the full release notes: `https://github.com/OpenLineage/OpenLineage/releases/tag/{RELEASE_VERSION}`
     - Thank contributors:
       - Mention the number of contributors if significant
       - Highlight new contributors if, based on the PR content, you can see that it was their first contribution to the OpenLineage project
     - Format suitable for Slack markdown (use `*` for bold, `` ` `` for code, etc.)

### 10. **Self-verification before completing:**
   - Verify all PR links are valid (format: `https://github.com/OpenLineage/OpenLineage/pull/{PR_NUMBER}`)
   - Verify all author links are valid (format: `https://github.com/{author}`) and point to actual PR authors
   - Check that `sidebar_position` in new website release file is exactly 1 less than the previous release file
   - Verify the date is today's date in YYYY-MM-DD format
   - Ensure excluded commits have clear, valid exclusion reasons
   - Verify the changelog format matches existing entries in `CHANGELOG.md`
   - Check that high-engagement PRs are properly highlighted in Slack message
   - Ensure the filename format matches existing release files (X_Y_Z.md)
   
   **After completing all tasks, provide a summary:**
   - Files created/modified
   - Number of commits processed
   - Number of PRs included/excluded
   - Any notable high-engagement PRs
   - Ready for review and commit
   - Remind the user to delete generated `release-*` files and never commit them to the repository

---

SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2025 contributors to the OpenLineage project
