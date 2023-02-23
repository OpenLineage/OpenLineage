### Using `get_changes`

The `get_changes.sh` script uses a fork of saadmk11/changelog-ci to get all 
merged changes between two specified releases. To get all changes since the latest
release, set `END_RELEASE_VERSION` to the planned next release. 

The changes will appear in this directory in a new file, CHANGES.md.

#### Requirements

Install dependencies with `pip install -r requirements.txt`.

The script also requires that the following environment variables be set:

`END_RELEASE_VERSION`
`START_RELEASE_VERSION`
`INPUT_GITHUB_TOKEN`

For example: `export END_RELEASE_VERSION=0.21.0`.

For instructions on creating a GitHub personal access token to use the GitHub API,
see: https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token.

#### Running the script

Run the script with `./get_changes.sh`.

If you get a `command not found` error, make sure you have made the script an
executable.

For example: `chmod u+x ./get_changes.sh`.