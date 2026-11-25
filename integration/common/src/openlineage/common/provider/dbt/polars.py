# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
"""Dataset identity for the dbt-polars adapter.

dbt-polars does not talk to a database server: it reads and writes tables through
"catalogs" declared in the profile. In dbt artifacts, a relation's ``database`` is the
*name the profile gives to a catalog* (an alias local to the project), not a physical
database. This module turns those aliases into OpenLineage dataset namespaces and names
that identify the physical Iceberg catalog instead.

Only Iceberg catalogs that pyiceberg reaches through a REST or SQL catalog are
supported. Any other catalog makes the lookup raise ``NotImplementedError`` instead of
producing a dataset that could be confused with another one.
"""

from __future__ import annotations

import ipaddress
import os
from typing import Any
from urllib.parse import SplitResult, parse_qs, unquote, urlsplit

import attr

_DEFAULT_PORTS = {"http": 80, "https": 443, "postgresql": 5432, "mysql": 3306, "mariadb": 3306}
# Azure storage URLs name the container (or OneLake workspace) where a URL has its user name.
_AZURE_STORAGE_SCHEMES = ("abfs", "abfss", "wasb", "wasbs")


@attr.define(frozen=True)
class PolarsCatalog:
    """The physical identity of one catalog entry of a dbt-polars profile.

    ``namespace`` identifies the catalog service and ``catalog`` (if any) the catalog
    inside it, which prefixes dataset names. ``namespace`` is ``None`` when the entry is
    not supported, and ``unsupported_reason`` says why.
    """

    alias: str
    namespace: str | None = None
    catalog: str | None = None
    unsupported_reason: str | None = None

    def dataset_identity(self, schema: str | None, table: str) -> tuple[str, str]:
        if self.namespace is None:
            raise NotImplementedError(
                f"dbt-polars catalog '{self.alias}' cannot be mapped to an OpenLineage dataset: "
                f"{self.unsupported_reason}"
            )
        return self.namespace, ".".join(filter(None, [self.catalog, schema, table]))


@attr.define
class PolarsCatalogs:
    """All catalogs of a dbt-polars profile, keyed by the alias used as dbt ``database``.

    ``entries`` holds, per alias, the profile entry and the key of its catalog type. An entry
    is resolved the first time it is used, so the options of a catalog no dataset uses, which
    may render Jinja such as an unset ``env_var``, are never read.
    """

    default: str
    entries: dict[str, tuple[Any, str]]
    project_dir: str | None = None
    _resolved: dict[str, PolarsCatalog] = attr.field(factory=dict, init=False)

    def get(self, alias: str | None) -> PolarsCatalog:
        # dbt-polars falls back to the default catalog for an empty database and
        # strips quotes from the catalog name; mirror both.
        name = (alias or "").strip('"') or self.default
        if name not in self._resolved:
            if name not in self.entries:
                raise NotImplementedError(
                    f"dbt-polars catalog '{name}' is not defined in the profile; "
                    f"known catalogs: {sorted(self.entries)}"
                )
            entry, type_key = self.entries[name]
            self._resolved[name] = _resolve_catalog(entry, name, entry.get(type_key), self.project_dir)
        return self._resolved[name]

    def dataset_identity(self, database: str | None, schema: str | None, table: str) -> tuple[str, str]:
        return self.get(database).dataset_identity(schema, table)

    @property
    def default_namespace(self) -> str:
        return self.get(self.default).namespace or ""


def parse_polars_profile(profile: dict[str, Any], project_dir: str | None = None) -> PolarsCatalogs:
    """Read the catalogs of a dbt-polars profile output.

    Accepts both profile shapes of dbt-polars: the single-catalog shorthand
    (``catalog_type`` plus the catalog options at the top level) and the
    ``catalogs`` list. The default catalog is ``database`` (or its alias ``catalog``),
    else the first catalog, as in dbt-polars. ``project_dir`` is the dbt project directory,
    which relative SQLite paths are resolved against.
    """
    default = profile.get("database") or profile.get("catalog")
    entries = profile.get("catalogs")
    if entries:
        if not isinstance(entries, list) or not all(isinstance(e, dict) and e.get("name") for e in entries):
            raise NotImplementedError("dbt-polars profile has a 'catalogs' entry without a name")
        catalogs = {str(e["name"]): (e, "type") for e in entries}
    else:
        if not profile.get("catalog_type"):
            raise NotImplementedError("dbt-polars profile must define either 'catalogs' or 'catalog_type'")
        # The profile is read in place: it may render Jinja lazily, and only the keys that
        # are needed are rendered, never secrets such as a token.
        catalogs = {str(default or "default"): (profile, "catalog_type")}

    default = str(default or next(iter(catalogs)))
    if default not in catalogs:
        raise NotImplementedError(f"dbt-polars default catalog '{default}' is not defined in 'catalogs'")
    return PolarsCatalogs(default=default, entries=catalogs, project_dir=project_dir)


def _resolve_catalog(
    entry: dict[str, Any], alias: str, catalog_type: Any, project_dir: str | None = None
) -> PolarsCatalog:
    def unsupported(reason: str) -> PolarsCatalog:
        return PolarsCatalog(alias=alias, unsupported_reason=reason)

    if catalog_type != "iceberg":
        return unsupported(f"catalog type '{catalog_type}' is not supported, only 'iceberg' is")
    if entry.get("py-catalog-impl"):
        return unsupported("custom pyiceberg catalog implementations (py-catalog-impl) are not supported")

    uri = entry.get("uri")
    uri = uri.strip() if isinstance(uri, str) else ""
    pyiceberg_type = entry.get("pyiceberg_type")
    if pyiceberg_type is None and uri:
        # Same inference as pyiceberg's load_catalog when no type is given.
        if uri.startswith("http"):
            pyiceberg_type = "rest"
        elif uri.startswith(("sqlite", "postgresql")):
            pyiceberg_type = "sql"
    pyiceberg_type = str(pyiceberg_type or "").lower()
    if pyiceberg_type not in ("rest", "sql"):
        return unsupported(
            f"pyiceberg catalog type '{pyiceberg_type or 'unknown'}' is not supported, "
            "only 'rest' and 'sql' are"
        )
    if not uri:
        # pyiceberg could still read it from ~/.pyiceberg.yaml or PYICEBERG_* variables,
        # which the profile does not show.
        return unsupported("the profile does not set the catalog 'uri'")

    try:
        if pyiceberg_type == "rest":
            parts = urlsplit(uri)
            if parts.scheme.lower() not in ("http", "https") or not parts.hostname:
                return unsupported("REST catalog uri is not an http(s) URL")
            namespace = _strip_credentials(parts)
            # pyiceberg sends every table request to {uri}/v1/{prefix}/..., so a prefix selects
            # a catalog of the service: the namespace is then that base endpoint.
            prefix = entry.get("prefix")
            prefix = str(prefix).strip().strip("/") if prefix is not None else ""
            if prefix:
                namespace = f"{namespace}/v1/{prefix}"
            warehouse = entry.get("warehouse")
            # A REST service can host several warehouses, so the warehouse is part of the identity.
            return PolarsCatalog(
                alias=alias,
                namespace=namespace,
                catalog=_normalize_warehouse(warehouse) if warehouse else None,
            )
        reason, sql_namespace = _sql_catalog_namespace(uri, project_dir)
        if sql_namespace is None:
            return unsupported(reason)
        return PolarsCatalog(alias=alias, namespace=sql_namespace, catalog=alias)
    except ValueError:
        return unsupported("the catalog uri is not a valid URL")


def _sql_catalog_namespace(uri: str, project_dir: str | None = None) -> tuple[str, str | None]:
    """The namespace of the database behind a SQLAlchemy URL, or why there is none.

    pyiceberg's SQL catalog keeps the tables of every catalog in one database and tells
    them apart by catalog name (``catalog`` of the dataset), so the namespace is the database.

    The URL is read as SQLAlchemy reads it: ``#`` is part of the database or query, not a
    fragment. The server can be given in the URL, in ``host``/``port`` query parameters
    (passed to the driver), or both; only a URL naming exactly one network server and at most
    one port is identifiable.
    """
    parts = urlsplit(uri, allow_fragments=False)
    scheme = parts.scheme.lower().split("+", 1)[0]  # drop the SQLAlchemy driver
    if not scheme:
        return "the SQL catalog uri has no scheme", None
    query = parse_qs(parts.query)

    if scheme == "sqlite":
        database = unquote(parts.path[1:])  # sqlite:///relative.db or sqlite:////absolute.db
        if parts.netloc or not database or ":memory:" in database or "memory" in query.get("mode", []):
            return "in-memory SQLite catalogs are not persistent", None
        # The namespace is the database file, however the profile spells it: dbt-polars opens
        # a relative path from the project directory.
        if not os.path.isabs(database):
            if not project_dir:
                return "a relative SQLite path needs the dbt project directory", None
            database = os.path.join(project_dir, database)
        database = os.path.realpath(database).replace(os.sep, "/")
        return "", f"sqlite:///{database}"  # sqlite:////abs/path, or sqlite:///C:/path on Windows

    # SQLAlchemy unquotes the database; psycopg passes the query to libpq, whose dbname wins.
    databases = query.get("dbname", []) if scheme == "postgresql" else []
    if len(databases) > 1:
        return "the SQL catalog uri names several databases", None
    database = databases[0] if databases else unquote(parts.path[1:])
    if not database:
        # The driver would pick a database, e.g. the one named after the user.
        return "the SQL catalog uri does not name a database", None

    authority = parts.netloc.rpartition("@")[2]
    hosts = query.get("host", [])
    if parts.hostname:
        hosts = [parts.hostname, *hosts]
    ports = query.get("port", [])
    if authority.rpartition("]")[2].count(":") > 0:  # an authority port, possibly empty
        ports = [authority.rpartition(":")[2], *ports]

    if "," in authority or len(hosts) != 1:
        return "the SQL catalog uri does not name a single database server", None
    host = hosts[0].strip()
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    if not host or host.startswith("/") or any(c in host for c in ",@/[]"):
        return "the SQL catalog uri does not name a single database server", None
    if len(ports) > 1 or not all(port.isdigit() and 0 < int(port) < 65536 for port in ports):
        return "the SQL catalog uri has an invalid port", None

    host = _canonical_host(host)
    if ports and int(ports[0]) != _DEFAULT_PORTS.get(scheme):
        host = f"{host}:{int(ports[0])}"
    return "", f"{scheme}://{host}/{database}"


def _canonical_host(host: str) -> str:
    """A lowercase host, with an IP address in its shortest form and IPv6 in brackets."""
    host = host.lower()
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return host
    return f"[{address.compressed}]" if address.version == 6 else address.compressed


def _normalize_warehouse(warehouse: Any) -> str:
    warehouse = str(warehouse).strip()
    if "://" in warehouse:
        # Some REST services take a storage location as warehouse.
        parts = urlsplit(warehouse)
        container = None
        if parts.scheme.lower() in _AZURE_STORAGE_SCHEMES and parts.password is None:
            container = parts.username
        return _strip_credentials(parts, user=container).rstrip("/")
    return warehouse


def _strip_credentials(parts: SplitResult, user: str | None = None) -> str:
    """Rebuild a URL without user info, query string or fragment.

    The scheme and host are lowercased, a default port and, for HTTP(S), a trailing slash
    are removed. ``user`` is kept in front of the host when given: for some storage URLs it
    names a container, not a user.
    """
    scheme = parts.scheme.lower()
    host = _canonical_host(parts.hostname or "")
    port = parts.port  # raises ValueError when invalid
    if port is not None and port != _DEFAULT_PORTS.get(scheme):
        host = f"{host}:{port}"
    if user:
        host = f"{user}@{host}"
    path = parts.path
    if scheme in ("http", "https"):
        path = path.rstrip("/")
    return f"{scheme}://{host}{path}"
