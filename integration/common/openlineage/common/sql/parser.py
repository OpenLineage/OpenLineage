# SPDX-License-Identifier: Apache-2.0.

import logging
from typing import List, Tuple, Optional

import sqlparse
from sqlparse.sql import T, TokenList, Parenthesis, Identifier, IdentifierList
from sqlparse.tokens import Punctuation


log = logging.getLogger(__name__)


def provider():
    return "python"


class DbTableMeta:
    def __init__(self, value: str):
        parts = value.strip().split('.')
        if len(parts) > 3:
            raise ValueError(
                f"Expected 'database.schema.table', found '{value}'."
            )
        self.database = self._get_database(parts)
        self.schema = self._get_schema(parts)
        self.name = self._get_table(parts)
        self.qualified_name = self._get_qualified_name()

    def has_database(self) -> bool:
        return self.database is not None

    def has_schema(self) -> bool:
        return self.schema is not None

    def _get_database(self, parts) -> str:
        # {database.schema.table}
        return parts[0] if len(parts) == 3 else None

    def _get_schema(self, parts) -> str:
        # {database.schema.table) or {schema.table}
        return parts[1] if len(parts) == 3 else (
            parts[0] if len(parts) == 2 else None
        )

    def _get_table(self, parts) -> str:
        # {database.schema.table} or {schema.table} or {table}
        return parts[2] if len(parts) == 3 else (
            parts[1] if len(parts) == 2 else parts[0]
        )

    def _get_qualified_name(self) -> str:
        return (
            f"{self.database}.{self.schema}.{self.name}"
            if self.has_database() else (
                f"{self.schema}.{self.name}" if self.has_schema() else self.name
            )
        )

    def __hash__(self):
        return hash(self.qualified_name)

    def __eq__(self, other):
        return self.database == other.database and \
               self.schema == other.schema and \
               self.name == other.name and \
               self.qualified_name == other.qualified_name

    def __repr__(self):
        # Return the string representation of the instance
        return (
            f"DbTableMeta({self.database!r},{self.schema!r},"
            f"{self.name!r},{self.qualified_name!r})"
        )

    def __str__(self):
        # Return the fully qualified table name as the string representation
        # of this object, otherwise the table name only
        return (
            self.qualified_name
            if (self.has_database() or self.has_schema()) else self.name
        )


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


def _get_tables(
        tokens,
        idx,
        default_schema: Optional[str] = None
) -> Tuple[int, List[DbTableMeta]]:
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

                # And again, to match bigquery's 'database.schema.table'
                try:
                    dot = next(token_list)
                    if dot.match(Punctuation, '.'):
                        table_name += dot.value
                        table_name += next(token_list).value
                except StopIteration:
                    # Do not insert database name if it's not specified
                    pass
            elif default_schema:
                table_name = f'{default_schema}.{table_name}'
        except StopIteration:
            if default_schema:
                table_name = f'{default_schema}.{table_name}'

        table_name = table_name.replace('`', '')
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

    return idx, [DbTableMeta(table) for table in tables]


class SqlMeta:
    def __init__(self, in_tables: List[DbTableMeta], out_tables: List[DbTableMeta]):
        self.in_tables = in_tables
        self.out_tables = out_tables

    def __repr__(self):
        return f"SqlMeta({self.in_tables!r},{self.out_tables!r})"

    def add_in_tables(self, in_tables: List[DbTableMeta]):
        for in_table in in_tables:
            self.in_tables.append(in_table)

    def add_out_tables(self, out_tables: List[DbTableMeta]):
        for out_table in out_tables:
            self.out_tables.append(out_table)


class SqlParser:
    """
    This class parses a SQL statement.
    """

    def __init__(self, default_schema: Optional[str] = None):
        # In some cases like bigquery we can always get schema/dataset ID from client
        self.default_schema = default_schema
        self.ctes = set()
        self.intables = set()
        self.outtables = set()

    def recurse(self, tokens: TokenList) -> SqlMeta:
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
                idx, extracted_tables = _get_tables(tokens, idx, self.default_schema)
                for table in extracted_tables:
                    if table.name not in self.ctes:
                        in_tables.add(table)
            elif _is_out_table(token):
                idx, extracted_tables = _get_tables(tokens, idx, self.default_schema)
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


def parse(
    sql: str,
    dialect: Optional[str] = None,
    default_schema: Optional[str] = None
) -> SqlMeta:
    if sql is None:
        raise ValueError("A sql statement must be provided.")

    # Tokenize the SQL statement
    sql_statements = sqlparse.parse(sql)

    sql_parser = SqlParser(default_schema)
    sql_meta = SqlMeta([], [])

    for sql_statement in sql_statements:
        tokens = TokenList(sql_statement.tokens)
        log.debug(f"Successfully tokenized sql statement: {tokens}")

        result = sql_parser.recurse(tokens)

        # Add the in / out tables (if any) to the sql meta
        sql_meta.add_in_tables(result.in_tables)
        sql_meta.add_out_tables(result.out_tables)

    return sql_meta
