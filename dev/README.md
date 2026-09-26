# Release tooling

The scripts in this directory are run via [Task](https://taskfile.dev) —
run `task --list` here to see all available tasks (or `task --list` at the
repo root for the `maintenance:`-prefixed versions). Python dependencies from
`requirements.txt` are provisioned automatically on each run, so no setup
step is needed.

All tasks require a GitHub personal access token. For instructions on
creating one, see:
https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token

To avoid passing the token on every invocation, copy `.env.template` in this
directory to `.env` (gitignored) and set it there; the tasks load it
automatically wherever they are invoked from.

### Updating the changelog

```sh
GITHUB_TOKEN=<token> task changes PREVIOUS=1.4.1 CURRENT=1.5.0
```

The changes between the two releases will appear in `../CHANGELOG.md`.

### Generating contributor stats

```sh
GITHUB_TOKEN=<token> task contributor-stats
```

The token needs `repo` rights. The date range defaults to the current month
(UTC); override it with `DATE_START=2026-05-01 DATE_END=2026-05-31`
(`%Y-%m-%d` format).

If all goes well, a table in csv format (`contributor_stats_table.csv`) will
appear in this directory.

### Creating a docs-site release doc

```sh
GITHUB_TOKEN=<token> task release-doc TAG=1.5.0
```

Prints the release doc for the given release tag to stdout.
