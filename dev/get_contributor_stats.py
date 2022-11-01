# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, List
import rich_click as click
import pendulum
from datetime import datetime
from dateutil.relativedelta import relativedelta
import textwrap
from github import Github
from github.Repository import Repository
import csv
from rich.console import Console

console = Console(width=400, color_system="standard")

option_github_token = click.option(
    "--github-token",
    type=str,
    required=True,
    help=textwrap.dedent(
        """
        A GitHub token is required, and can also be provided by setting the GITHUB_TOKEN env variable.
        Can be generated with:
        https://github.com/settings/tokens/new?description=Read%20issues&scopes=repo:status"""
    ),
    envvar='GITHUB_TOKEN',
)

class ContributorStats:

    def __init__(self, repo: Repository):
        self.repo = repo
        self.rows = []
        self.logins = {}
        self.committer_prs = []
        self.committer_changes = []
        self.committer_avg = 0

    def get_stats(self, date_start, date_end):
        rows = []
        for contributor in self.repo.get_stats_contributors():
            name = contributor.author.name
            login = contributor.author.login
            email = contributor.author.email
            alltime_commits = contributor.total
            alltime_additions = 0
            alltime_deletions = 0
            period_commits = 0
            period_additions = 0
            period_deletions = 0
            for week in contributor.weeks:
                alltime_additions += week.a
                alltime_deletions += week.d
                if week.w > date_start and week.w < date_end:     
                    period_additions += week.a
                    period_deletions += week.d
                    period_commits += week.c
            row = [
                name, 
                login, 
                email, 
                alltime_additions, 
                alltime_deletions, 
                alltime_commits, 
                period_additions, 
                period_deletions, 
                period_commits
            ]
            rows.append(row)
        self.rows = rows
    
    def sort_key(self, row):
        """key for sorting contributors by total commits during period"""
        return row[8]

    def sort_contributors(self):
        """sorts contributors using key"""
        self.rows = sorted(self.rows, key=self.sort_key)

    def get_pulls(self, date_start, date_end):
        """get the number of PRs closed by each contributor"""
        logins = {}
        for row in self.rows:
            logins[row[1]] = {'pulls':0, 'total':0}
        console.print(f'Getting pulls closed between {date_start} and {date_end}...', style='magenta')
        pulls = self.repo.get_pulls(state="closed", sort="created", direction="desc")
        for pull in pulls:
            if pull.user.login in logins.keys():
                if pull.closed_at > date_start and pull.closed_at < date_end:
                    logins[pull.user.login]['pulls'] += 1
                    commit = self.repo.get_commit(sha=pull.merge_commit_sha)
                    logins[pull.user.login]['total'] += commit.stats.total
            else:
                if pull.closed_at > date_start and pull.closed_at < date_end:
                    logins[pull.user.login] = {'pulls':1, 'total':0}
                    commit = self.repo.get_commit(sha=pull.merge_commit_sha)
                    logins[pull.user.login]['total'] += commit.stats.total
        self.logins = logins

    def add_pulls(self):
        """adds PR data to rows dataset"""
        console.print('Adding PRs to dataset and exporting table...', style='green')
        rows = self.rows
        for row in rows:
            row.append(self.logins[row[1]]['pulls'])
        self.rows = rows

    def collect_committers(self):
        """collect stats about active contributors for comparison"""
        committers = (
            'Julien Le Dem', 
            'Mandy Chessell', 
            'Daniel Henneberger', 
            'Drew Banin', 
            'James Campbell', 
            'Ryan Blue', 
            'Willy Lulciuc', 
            'Zhamak Dehghani', 
            'Michael Collado', 
            'Maciej Obuchowski', 
            'pawel.leszczynski', 
            'Will Johnson', 
            'Michael Robinson', 
            'Ross Turk'
            )
        committer_changes = []
        committer_prs = []
        rows = self.rows
        for row in rows:
            if row[0] in committers:
                if row[8] > 0:
                    committer_changes.append( (row[6]+row[7])/row[8] )
                    committer_prs.append(self.logins[row[1]]['pulls'])
                row.append('committer')
            else:
                row.append('non-committer')
        if len(committer_changes) > 0:
            committer_avg = sum(committer_changes)/len(committer_changes)
        else:
            committer_avg = 'N/A'
        self.committer_changes = committer_changes
        self.committer_prs = committer_prs
        self.committer_avg = committer_avg

    def compare_committers(self):
        """calculate diff between contributor's average changes and average for all active committers"""
        rows = self.rows
        for row in rows:
            if row[8] > 0:
                avg = (row[6]+row[7])/row[8]
                avg_diff = 'N/A'
                if self.committer_avg != 'N/A':
                    avg_diff = ( (row[6]+row[7])/row[8] ) - self.committer_avg
                row.append(avg)
                row.append(avg_diff)
            else:
                row.append('N/A')
                row.append('N/A')
        if len(self.committer_prs) > 0 and len(self.committer_changes) > 0:
            print('Average # of PRs by committers during period: ', sum(self.committer_prs)/len(self.committer_prs))
            print('Average size of commits by committers active during period: ', sum(self.committer_changes)/len(self.committer_changes))
        self.rows = rows

    def verbose_str(self):
        """output a detailed ranked list to the terminal"""
        rank = 1
        for row in reversed(self.rows):    
            console.print(rank, ': ', row[0])
            print('login: ', row[1])
            if isinstance(row[2], str):
                print('email: ', row[2])
            print('total additions (all-time): ', row[3])
            print('total deletions (all-time): ', row[4])
            print('total changes (all-time): ', row[3] + row[4])
            print('total commits (all-time): ', row[5])
            print('total additions this period: ', row[6])
            print('total deletions this period: ', row[7])
            print('total changes this period: ', row[6] + row[7])
            print('total commits this period: ', row[8])
            print('total PRs this period: ', self.logins[row[1]]['pulls'])
            print('average commit size this period: ', row[11])
            print('average commit size relative to active committer average: ', row[12])
            console.print('-------------', style='green')
            rank += 1

    def export_csv(self):
        """write the data to a local .csv file"""
        console.print('Exporting to .csv file...', style='blue')
        header = [
            'name', 
            'username', 
            'email', 
            'all-time additions', 
            'all-time deletions', 
            'all-time commits', 
            'additions this period', 
            'deletions this period', 
            'commits this period', 
            'PRs this period', 
            'committer status', 
            'average commit size this period', 
            'changes relative to committer average for period']
        with open('contributor_stats_table.csv', 'w+', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(self.rows)
            f.close()

today = pendulum.today().date()
first_day = today.replace(day=1)
DEFAULT_BEGINNING_OF_MONTH=first_day
DEFAULT_END_OF_MONTH=DEFAULT_BEGINNING_OF_MONTH + relativedelta(months=+1)

@click.command()
@option_github_token  # TODO: this should only be required if --load isn't provided
@click.option(
    '--date-start', type=click.DateTime(formats=["%Y-%m-%d"]), default=str(DEFAULT_BEGINNING_OF_MONTH)
)
@click.option(
    '--date-end', type=click.DateTime(formats=["%Y-%m-%d"]), default=str(DEFAULT_END_OF_MONTH)
)
@click.option('--verbose', is_flag="True", help="Print details")
@click.option('--repo', help="Search org/repo", default=str("openlineage/OpenLineage"))

def main(
    github_token: str,
    date_start: datetime,
    date_end: datetime,
    verbose: bool,
    repo: str
):    
    g = Github(github_token)
    repo = g.get_repo(repo)
    if repo:
        console.print('GitHub repo found, getting contributor stats...', style="green")
        stats = ContributorStats(repo=repo)
        stats.get_stats(date_start=date_start, date_end=date_end)
        stats.sort_contributors()
        stats.get_pulls(date_start=date_start, date_end=date_end)
        stats.add_pulls()
        stats.collect_committers()
        stats.compare_committers()
        if verbose:
            stats.verbose_str()
        else:
            print('skipping output...')
        stats.export_csv()
        console.print('Done!', style='green')
    else:
        print('Oops, there was a problem accessing the GitHub repo.')
        exit()

if __name__ == "__main__":
    main()