### Using `get_changes`

The `get_changes.sh` script uses a fork of saadmk11/changelog-ci to get all 
merged changes between two specified releases. To get all changes since the latest
release, set `END_RELEASE_VERSION` to the planned next release. 

The changes will appear in this directory in a new file, CHANGES.md.

The script requires that the following environment variables be set:

`END_RELEASE_VERSION`
`START_RELEASE_VERSION`
`INPUT_GITHUB_TOKEN`

For example: `export END_RELEASE_VERSION=0.21.0`.