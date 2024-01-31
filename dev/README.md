### Using `get_changes`

The changes will appear in ../CHANGELOG.md.

#### Requirements

Install the required dependencies in requirements.txt.

The script also requires a GitHub token.

For instructions on creating a GitHub personal access token to use the GitHub API,
see: https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token.

#### Running the script

Run the script from the root directory: 

```sh
python3 dev/get_changes.py --github_token token --previous 1.4.1 --current 1.5.0
```

If you get a `command not found` or similar error, make sure you have made the 
script an executable (e.g., `chmod u+x ./dev/get_changes.py`).

### Using `get_contributor_stats`

A GitHub token with `repo` rights is required (pass it via the `--github-token` parameter).

The start date defaults to the beginning of the current month, and the end date defaults to the last day of the current month (UTC).

These dates can be changed via the `--date-start` and `--date-end` parameters in `%Y-%m-%d` format.

If all goes well, a table in csv format will appear in this directory.