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

import sqlparse
from sqlparse.sql import T
from sqlparse.sql import TokenList
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


def _get_table(tokens, idx):
    idx, token = tokens.token_next(idx=idx)
    token_list = token.flatten()
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
    return idx, DbTableName(table_name)


class SqlMeta:
    # TODO: Only a single output table may exist, we'll want to rename
    # SqlMeta.out_tables -> SqlMeta.out_table
    def __init__(self, in_tables: [DbTableName], out_tables: [DbTableName]):
        self.in_tables = in_tables
        self.out_tables = out_tables


class SqlParser:
    """
    This class parses a SQL statement.
    """

    @staticmethod
    def parse(sql: str) -> SqlMeta:
        if sql is None:
            raise ValueError("A sql statement must be provided.")

        # Tokenize the SQL statement
        statements = sqlparse.parse(sql)

        # We assume only one statement in SQL
        tokens = TokenList(statements[0].tokens)
        log.debug(f"Successfully tokenized sql statement: {tokens}")

        in_tables = []
        out_tables = []

        idx, token = tokens.token_next_by(t=T.Keyword)
        while token:
            if _is_in_table(token):
                idx, in_table = _get_table(tokens, idx)
                in_tables.append(in_table)
            elif _is_out_table(token):
                idx, out_table = _get_table(tokens, idx)
                out_tables.append(out_table)

            idx, token = tokens.token_next_by(t=T.Keyword, idx=idx)

        return SqlMeta(in_tables, out_tables)
