#!/usr/bin/env python3
#
# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from github import Github
import rich_click as click
from datetime import date
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from github.PullRequest import PullRequest

class GetChanges:

    def __init__(self, github_token: str, previous: str, current: str, path: str):
        self.github_token = github_token
        self.previous = previous
        self.current = current
        self.path = path
        self.pulls: list[PullRequest] = []
        self.rel_title_str: str = ''
        self.text: list[str] = []
        self.new_contributors: dict[str:str] = {}

    def get_pulls(self):
        print('Working on it...')
        g = Github(self.github_token)
        repo = g.get_repo("OpenLineage/openlineage")
        prev_date = repo.get_release(self.previous).created_at
        commits = repo.get_commits(since=prev_date)
        self.pulls = [pull for commit in commits for pull in commit.get_pulls()]
    
    def write_title(self):
        self.rel_title_str = f'## [{self.current}](https://github.com/OpenLineage/OpenLineage/compare/{self.previous}...{self.current}) - {date.today()}'
            
    def describe_changes(self):
        for pull in self.pulls:

            """ Assembles change description with PR and user URLs """
            entry = []
            if pull.user.login != 'dependabot[bot]':
                labels = []
                for label in pull.labels:
                    if label.name != 'documentation':
                        labels.append(label.name)
                change_str = f'* **{labels[0]}: {pull.title}** [`#{pull.number}`]({pull.html_url}) [@{pull.user.login}]({pull.user.html_url})  '
                
                """ Extracts one-line description if present """
                beg = pull.body.find('One-line summary:') + 18
                if beg == 17:
                    change_descrip_str = '    **'
                else:
                    test = pull.body.find('### Checklist')
                    if test == -1:
                        end = beg + 75
                    else:
                        end = test - 1
                    descrip = pull.body[beg:end].split()
                    descrip_str = ' '.join(descrip)
                    change_descrip_str = f'    *{descrip_str}*'
                
                entry.append(change_str + '\n')
                entry.append(change_descrip_str + '\n')
                self.text.append(entry)

    def get_new_contributors(self):
        for pull in self.pulls:
            comments = pull.get_issue_comments()
            for comment in comments:
                if 'Thanks for opening your' in comment.body:
                    self.new_contributors[pull.user.login] = pull.user.url
        if self.new_contributors:
            print('New contributors:')
            for k, v in self.new_contributors.items():
                print(f'@{k}: {v}')
        else:
            print('Note: no new contributors were identified.')

    def update_changelog(self):
        f = open('changes.txt', 'w+')
        f = open('changes.txt', 'a')
        f.write(self.rel_title_str + '\n')
        for entry in self.text:
            for line in entry:
                f.write(line)
        f.close()

        with open('changes.txt', 'r+') as f:
            new_changes = f.read()
        with open(self.path, 'r') as contents:
            save = contents.read()
        with open(self.path, 'w') as contents:
            contents.write(new_changes)
        with open(self.path, 'a') as contents:
            contents.write(save)

@click.command()
@click.option(
    '--github_token', type=str, default=''
)
@click.option(
    "--previous", type=str, default=''
)
@click.option(
    "--current", type=str, default=''
)
@click.option(
    "--path", 
    type=str, 
    default='/Users/michael/Documents/astronomer_projects/openlineage_upstream/OpenLineage/CHANGELOG.md',
    help='absolute path to changelog',
)

def main(
    github_token: str,
    previous: str,
    current: str,
    path: str,
):
    c = GetChanges(
        github_token=github_token, 
        previous=previous, 
        current=current, 
        path=path
    )
    c.get_pulls()
    c.describe_changes()
    c.write_title()
    c.update_changelog()
    c.get_new_contributors()
    print('...done!')

if __name__ == "__main__":
    main()
