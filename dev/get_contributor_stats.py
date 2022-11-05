# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import rich_click as click
import pendulum
from datetime import datetime
from dateutil.relativedelta import relativedelta
import textwrap
from github import Github
from github.Repository import Repository
from github.Organization import Organization
from github.Team import Team
import csv
from rich.console import Console

console = Console(width=400, color_system="auto")

option_github_token = click.option(
    "--github-token",
    type=str,
    required=True,
    help=textwrap.dedent(
        """
        A GitHub token is required, and can also be provided by setting the GITHUB_TOKEN env variable.
        Can be generated with:
        https://github.com/settings/tokens/new?description=Study%20Committers&scopes=repo:status%2Cread:org
        """
    ),
    envvar='GITHUB_TOKEN',
)

class ContributorStats:

    def __init__(self, repo: Repository, org: Organization, team: Team, sort: str):
        self.repo = repo
        self.org = org
        self.team = team
        self.sort = sort
        self.rows = []
        self.logins = {}
        self.committer_prs = []
        self.committer_changes = []
        self.committer_avg = 0
        self.committer_avg_prs = 0
        self.committers = {}

    def get_stats(self, date_start, date_end):
        """gets commit stats for all contributors"""
        console.print(
            'GitHub repo found, getting historical stats...', 
            style="green"
            )
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
                if date_start < week.w < date_end:     
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
            self.rows.append(row)

    def sort_key(self, row):
        """provides key for sorting contributors by total commits during period"""
        if self.sort == 'commits-this-period':
            return row[8]
        elif self.sort == 'PRs':
            return row[9]
        elif self.sort == 'all-time-commits':
            return row[5]
        elif self.sort == 'all-time-additions':
            return row[3]
        elif self.sort == 'additions-this-period':
            return row[6]
        elif self.sort == 'deletions-this-period':
            return row[7]
        elif self.sort == 'all-time-deletions':
            return row[4]

    def sort_contributors(self):
        """sorts contributors using key"""
        if self.sort == 'commits-this-period':
            console.print('Sorting by commits this period...', style='green')
        elif self.sort == 'PRs':
            console.print('Sorting by PRs this period...', style='green')
        elif self.sort == 'all-time-commits':
            console.print('Sorting by all-time commits...', style='green')
        elif self.sort == 'all-time-additions':
            console.print('Sorting by all-time additions...', style='green')
        elif self.sort == 'additions-this-period':
            console.print('Sorting by additions this period...', style='green')
        elif self.sort == 'deletions-this-period':
            console.print('Sorting by deletions this period...', style='green')
        elif self.sort == 'all-time-deletions':
            console.print('Sorting by all-time deletions...', style='green')
        self.rows = sorted(self.rows, key=self.sort_key)

    def get_pulls(self, date_start, date_end):
        """gets the number of PRs closed by each contributor"""
        for row in self.rows:
            self.logins[row[1]] = {'pulls':0, 'total':0}
        console.print(
            f'Getting pulls closed between {date_start} and {date_end}...', 
            style='magenta'
            )
        pulls = self.repo.get_pulls(
            state="closed", 
            sort="created", 
            direction="desc"
            )
        for pull in pulls:
            if pull.user.login in self.logins.keys():
                if date_start < pull.closed_at < date_end:
                    self.logins[pull.user.login]['pulls'] += 1
                    commit = self.repo.get_commit(sha=pull.merge_commit_sha)
                    self.logins[pull.user.login]['total'] += commit.stats.total
            else:
                if date_start < pull.closed_at < date_end:
                    self.logins[pull.user.login] = {'pulls':1, 'total':0}
                    commit = self.repo.get_commit(sha=pull.merge_commit_sha)
                    self.logins[pull.user.login]['total'] += commit.stats.total

    def add_pulls(self):
        """adds PR data to rows dataset"""
        console.print(
            'Adding PRs to dataset and exporting table...', 
            style='green'
            )
        for row in self.rows:
            row.append(self.logins[row[1]]['pulls'])

    def collect_committers(self):
        """collects stats about active committers for comparison"""
        team = self.org.get_team_by_slug(self.team)
        committers = []
        for member in team.get_members():
            committers.append(member.login)
        for row in self.rows:
            if row[1] in committers:
                if row[8] > 0:
                    self.committer_changes.append( (row[6]+row[7])/row[8] )
                    self.committer_prs.append(self.logins[row[1]]['pulls'])
                row.append('committer')
            else:
                row.append('non-committer')
        if len(self.committer_changes) > 0:
            self.committer_avg = round(
                sum(self.committer_changes)/len(self.committer_changes), 2)
        else:
            self.committer_avg = 'N/A'
        if len(self.committer_prs) > 0:
            self.committer_avg_prs = round(
                sum(self.committer_prs)/len(self.committer_prs), 
                2
                )

    def compare_committers(self):
        """calculates diff between contributor's average changes and average for all active committers"""
        for row in self.rows:
            if row[8] > 0:
                avg = round((row[6]+row[7])/row[8], 2)
                avg_diff = 'N/A'
                if self.committer_avg != 'N/A':
                    avg_diff = round(
                        ( (row[6]+row[7])/row[8] ) - self.committer_avg, 2)
                row.append(avg)
                row.append(avg_diff)
                if self.committer_avg_prs != 'N/A':
                    avg_diff_prs = round(row[9] - self.committer_avg_prs, 2)
                row.append(avg_diff_prs)
            else:
                row.append('N/A')
                row.append('N/A')
                row.append('N/A')
        if len(self.committer_prs) > 0 and len(self.committer_changes) > 0:
            console.print(
                'Average # of PRs by committers during period: ', 
                round(sum(self.committer_prs)/len(self.committer_prs), 2),
                style='green'
                )
            console.print(
                'Average size of commits by committers active during period: ', 
                round(sum(self.committer_changes)/len(self.committer_changes), 2),
                style='green'
                )

    def verbose_str(self):
        """outputs a detailed ranked list to the terminal"""
        rank = 1
        for row in reversed(self.rows):    
            console.print(rank, ': ', row[0], style='yellow')
            console.print('login: ', row[1], style='yellow')
            if isinstance(row[2], str):
                console.print('email: ', row[2], style='yellow')
            console.print('total additions (all-time): ', row[3], style='cyan')
            console.print('total deletions (all-time): ', row[4], style='cyan')
            console.print('total changes (all-time): ', row[3] + row[4], style='cyan')
            console.print('total commits (all-time): ', row[5], style='cyan')
            console.print('total additions this period: ', row[6], style='green')
            console.print('total deletions this period: ', row[7], style='green')
            console.print(
                'total changes this period: ', 
                row[6] + row[7], 
                style='green'
                )
            console.print('total commits this period: ', row[8], style='green')
            console.print('total PRs this period: ', row[9], style='green')
            console.print(
                'average commit size this period: ', 
                row[11], 
                style='green'
                )
            console.print(
                'average commit size relative to active committer average: ', 
                row[12], 
                style='magenta'
                )
            console.print(
                'number of PRs this period relative to active committer average: ', 
                row[13], 
                style='magenta'
                )
            console.print('-------------', style='green')
            rank += 1

    def terse_str(self):
        """outputs a terse ranked list to the terminal"""
        rank = 1
        for row in reversed(self.rows):    
            console.print(rank, ': ', row[0], style='yellow')
            console.print('login: ', row[1], style='magenta')
            rank += 1

    def export_csv(self):
        """writes the data to a local .csv file"""
        print('Exporting to .csv file...')
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
            'changes relative to committer average for period',
            'PRs relative to committer average for period'
            ]
        with open(
            'contributor_stats_table.csv', 
            'w+', 
            encoding='UTF8', 
            newline=''
            ) as f:
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
    '--date-start', 
    type=click.DateTime(formats=["%Y-%m-%d"]), 
    default=str(DEFAULT_BEGINNING_OF_MONTH)
)
@click.option(
    '--date-end', 
    type=click.DateTime(formats=["%Y-%m-%d"]), 
    default=str(DEFAULT_END_OF_MONTH)
)
@click.option('--verbose', is_flag="True", help="Print details")
@click.option(
    '--repo', 
    help="Search org/repo", 
    default=str("openlineage/OpenLineage")
    )
@click.option(
    '--org', 
    help="Search org", 
    default=str("openlineage")
    )
@click.option(
    '--team',
    help="Search org for committer team name", 
    default=str("committers")
)
@click.option(
    '--sort',
    help=
    """
    Sort options: PRs, commits-this-period, all-time-commits, all-time-additions, 
    additions-this-period, deletions-this-period, all-time-deletions
    """,
    default=str("PRs")
)

def main(
    github_token: str,
    date_start: datetime,
    date_end: datetime,
    verbose: bool,
    repo: str,
    org: str,
    team: str,
    sort: str
):    
    with console.status('Working...', spinner='line'):    
        console.print(
            'Fetching GitHub repo and org...', 
            style="green"
            )
        g = Github(github_token)
        repo = g.get_repo(repo)
        org = g.get_organization(org)
        if repo:
            stats = ContributorStats(repo=repo, org=org, team=team, sort=sort)
            stats.get_stats(date_start=date_start, date_end=date_end)
            stats.get_pulls(date_start=date_start, date_end=date_end)
            stats.add_pulls()
            stats.sort_contributors()
            stats.collect_committers()
            stats.compare_committers()
            if verbose:
                stats.verbose_str()
            else:
                stats.terse_str()
            stats.export_csv()
            console.print('Done!', style='green')
        else:
            print('Oops, there was a problem accessing the GitHub repo.')
            exit()

if __name__ == "__main__":
    main()