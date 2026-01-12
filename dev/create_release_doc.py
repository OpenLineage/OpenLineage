#!/usr/bin/env python3
#
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import rich_click as click
from github import Github


class DocRelease:
    """A script for generating a release doc for the OpenLineage Docs site."""

    def __init__(self, repo, tag: str):
        self.repo = repo
        self.tag: str = tag
        self.release: str = ""
        self.release_date: str = ""

    def get_release(self):
        self.release = self.repo.get_release(self.tag).body
        self.release_date = self.repo.get_release(self.tag).created_at.strftime("%Y-%m-%d")

    def create_doc(self):
        doc = f"# {self.tag} - {self.release_date}" + "\n" * 2 + self.release
        print(doc)


@click.command()
@click.option(
    "--token",
    type=str,
    default="",
)
@click.option(
    "--tag",
    type=str,
    default="",
)
def main(
    token: str,
    tag: str,
):
    g = Github(token)
    repo = g.get_repo("OpenLineage/openlineage")
    c = DocRelease(repo, tag)
    c.get_release()
    c.create_doc()


if __name__ == "__main__":
    main()
