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


class DbColumn:
    def __init__(self, name: str, type: str,
                 description: str = None, ordinal_position: int = None):
        self.name = name
        self.type = type
        self.description = description
        self.ordinal_position = ordinal_position

    def __eq__(self, other):
        return self.name == other.name and \
               self.type == other.type and \
               self.description == other.description and \
               self.ordinal_position == other.ordinal_position

    def __repr__(self):
        return f"DbColumn({self.name!r},{self.type!r}, \
                          {self.description!r},{self.ordinal_position!r})"


class DbTableName:
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
                f"{self.schema}.{self.name}" if self.has_schema() else None
            )
        )

    def __hash__(self):
        return hash((self.database, self.schema, self.name, self.qualified_name))

    def __eq__(self, other):
        return self.database == other.database and \
               self.schema == other.schema and \
               self.name == other.name and \
               self.qualified_name == other.qualified_name

    def __repr__(self):
        # Return the string representation of the instance
        return (
            f"DbTableName({self.database!r},{self.schema!r},"
            f"{self.name!r},{self.qualified_name!r})"
        )

    def __str__(self):
        # Return the fully qualified table name as the string representation
        # of this object, otherwise the table name only
        return (
            self.qualified_name
            if (self.has_database() or self.has_schema()) else self.name
        )


class DbTableSchema:
    def __init__(self, schema_name: str, table_name: DbTableName,
                 columns: [DbColumn]):
        self.schema_name = schema_name
        self.table_name = table_name
        self.columns = columns

    def __eq__(self, other):
        return self.schema_name == other.schema_name and \
               self.table_name == other.table_name and \
               self.columns == other.columns

    def __repr__(self):
        return f"DbTableSchema({self.schema_name!r},{self.table_name!r}, \
                               {self.columns!r})"
