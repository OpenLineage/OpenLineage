# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from typing import List, Tuple

import sqlparse
from sqlparse.sql import T, TokenList, Parenthesis, Identifier, IdentifierList
from sqlparse.tokens import Punctuation

from marquez_airflow.models import DbTableName

log = logging.getLogger(__name__)


def _is_in_table(token):
    return _match_on(token, [
        'FROM',
        'INNER JOIN',
        'JOIN',
        'FULL JOIN',
        'FULL OUTER JOIN',
        'LEFT JOIN',
        'LEFT OUTER JOIN',
        'RIGHT JOIN',
        'RIGHT OUTER JOIN'
    ])


def _is_out_table(token):
    return _match_on(token, ['INTO'])


def _match_on(token, keywords):
    return token.match(T.Keyword, values=keywords)


def _get_tables(tokens, idx) -> Tuple[int, List[DbTableName]]:
    # Extract table identified by preceding SQL keyword at '_is_in_table'
    def parse_ident(ident: Identifier) -> str:
        # Extract table name from possible schema.table naming
        token_list = ident.flatten()
        table_name = next(token_list).value
        try:
            # Determine if the table contains the schema
            # separated by a dot (format: 'schema.table')
            dot = next(token_list)
            if dot.match(Punctuation, '.'):
                table_name += dot.value
                table_name += next(token_list).value
        except StopIteration:
            pass
        return table_name

    idx, token = tokens.token_next(idx=idx)
    tables = []
    if isinstance(token, IdentifierList):
        # Handle "comma separated joins" as opposed to explicit JOIN keyword
        gidx = 0
        tables.append(parse_ident(token.token_first(skip_ws=True, skip_cm=True)))
        gidx, punc = token.token_next(gidx, skip_ws=True, skip_cm=True)
        while punc and punc.value == ',':
            gidx, name = token.token_next(gidx, skip_ws=True, skip_cm=True)
            tables.append(parse_ident(name))
            gidx, punc = token.token_next(gidx)
    else:
        tables.append(parse_ident(token))

    return idx, [DbTableName(table) for table in tables]


class SqlMeta:
    # TODO: Only a single output table may exist, we'll want to rename
    # SqlMeta.out_tables -> SqlMeta.out_table
    def __init__(self, in_tables: List[DbTableName], out_tables: List[DbTableName]):
        self.in_tables = in_tables
        self.out_tables = out_tables


class SqlParser:
    """
    This class parses a SQL statement.
    """

    def __init__(self):
        self.ctes = set()
        self.intables = set()
        self.outtables = set()

    @classmethod
    def parse(cls, sql: str) -> SqlMeta:
        if sql is None:
            raise ValueError("A sql statement must be provided.")

        # Tokenize the SQL statement
        statements = sqlparse.parse(sql)

        # We assume only one statement in SQL
        tokens = TokenList(statements[0].tokens)
        log.debug(f"Successfully tokenized sql statement: {tokens}")
        parser = cls()
        return parser.recurse(tokens)

    def recurse(self, tokens: TokenList):
        in_tables, out_tables = set(), set()
        idx, token = tokens.token_next_by(t=T.Keyword)
        while token:

            # Main parser switch
            if self.is_cte(token):
                cte_name, cte_intables = self.parse_cte(idx, tokens)
                for intable in cte_intables:
                    if intable not in self.ctes:
                        in_tables.add(intable)
            elif _is_in_table(token):
                idx, extracted_tables = _get_tables(tokens, idx)
                for table in extracted_tables:
                    if table.name not in self.ctes:
                        in_tables.add(table)
            elif _is_out_table(token):
                idx, extracted_tables = _get_tables(tokens, idx)
                out_tables.add(extracted_tables[0])  # assuming only one out_table

            idx, token = tokens.token_next_by(t=T.Keyword, idx=idx)

        return SqlMeta(list(in_tables), list(out_tables))

    def is_cte(self, token: T):
        return token.match(T.Keyword.CTE, values=['WITH'])

    def parse_cte(self, idx, tokens: TokenList):
        gidx, group = tokens.token_next(idx, skip_ws=True, skip_cm=True)

        # handle recursive keyword
        if group.match(T.Keyword, values=['RECURSIVE']):
            gidx, group = tokens.token_next(gidx, skip_ws=True, skip_cm=True)

        if not group.is_group:
            return [], None

        # get CTE name
        offset = 1
        cte_name = group.token_first(skip_ws=True, skip_cm=True)
        self.ctes.add(cte_name.value)

        # AS keyword
        offset, as_keyword = group.token_next(offset, skip_ws=True, skip_cm=True)
        if not as_keyword.match(T.Keyword, values=['AS']):
            raise RuntimeError(f"CTE does not have AS keyword at index {gidx}")

        offset, parens = group.token_next(offset, skip_ws=True, skip_cm=True)
        if isinstance(parens, Parenthesis) or parens.is_group:
            # Parse CTE using recursion.
            return cte_name.value, self.recurse(TokenList(parens.tokens)).in_tables
        raise RuntimeError(f"Parens {parens} are not Parenthesis at index {gidx}")
