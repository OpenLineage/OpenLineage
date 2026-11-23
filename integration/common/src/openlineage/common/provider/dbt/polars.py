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

from typing import Any
from urllib.parse import SplitResult, parse_qs, urlsplit

import attr

_DEFAULT_PORTS = {"http": 80, "https": 443}


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


@attr.define(frozen=True)
class PolarsCatalogs:
    """All catalogs of a dbt-polars profile, keyed by the alias used as dbt ``database``."""

    default: str
    catalogs: dict[str, PolarsCatalog]

    def get(self, alias: str | None) -> PolarsCatalog:
        # dbt-polars falls back to the default catalog for an empty database and
        # strips quotes from the catalog name; mirror both.
        name = (alias or "").strip('"') or self.default
        catalog = self.catalogs.get(name)
        if catalog is None:
            raise NotImplementedError(
                f"dbt-polars catalog '{name}' is not defined in the profile; "
                f"known catalogs: {sorted(self.catalogs)}"
            )
        return catalog

    def dataset_identity(self, database: str | None, schema: str | None, table: str) -> tuple[str, str]:
        return self.get(database).dataset_identity(schema, table)

    @property
    def default_namespace(self) -> str:
        return self.catalogs[self.default].namespace or ""


def parse_polars_profile(profile: dict[str, Any]) -> PolarsCatalogs:
    """Read the catalogs of a dbt-polars profile output.

    Accepts both profile shapes of dbt-polars: the single-catalog shorthand
    (``catalog_type`` plus the catalog options at the top level) and the
    ``catalogs`` list. The default catalog is ``database`` (or its alias ``catalog``),
    else the first catalog, as in dbt-polars.
    """
    default = profile.get("database") or profile.get("catalog")
    entries = profile.get("catalogs")
    if entries:
        if not isinstance(entries, list) or not all(isinstance(e, dict) and e.get("name") for e in entries):
            raise NotImplementedError("dbt-polars profile has a 'catalogs' entry without a name")
    else:
        catalog_type = profile.get("catalog_type")
        if not catalog_type:
            raise NotImplementedError("dbt-polars profile must define either 'catalogs' or 'catalog_type'")
        entries = [{**profile, "name": default or "default", "type": catalog_type}]

    catalogs = {str(entry["name"]): _resolve_catalog(entry) for entry in entries}
    default = str(default or entries[0]["name"])
    if default not in catalogs:
        raise NotImplementedError(f"dbt-polars default catalog '{default}' is not defined in 'catalogs'")
    return PolarsCatalogs(default=default, catalogs=catalogs)


def _resolve_catalog(entry: dict[str, Any]) -> PolarsCatalog:
    alias = str(entry["name"])

    def unsupported(reason: str) -> PolarsCatalog:
        return PolarsCatalog(alias=alias, unsupported_reason=reason)

    if entry.get("type") != "iceberg":
        return unsupported(f"catalog type '{entry.get('type')}' is not supported, only 'iceberg' is")
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

    if pyiceberg_type == "rest":
        parts = urlsplit(uri)
        if parts.scheme.lower() not in _DEFAULT_PORTS or not parts.hostname:
            return unsupported(f"REST catalog uri is not an http(s) URL: {_strip_credentials(parts)}")
        warehouse = entry.get("warehouse")
        # A REST service can host several warehouses, so the warehouse is part of the identity.
        return PolarsCatalog(
            alias=alias,
            namespace=_strip_credentials(parts),
            catalog=_normalize_warehouse(warehouse) if warehouse else None,
        )
    # SQL catalog. pyiceberg's SQL catalog keeps the tables of every catalog in one database
    # and tells them apart by catalog name, which dbt-polars sets to the catalog's name in
    # the profile.
    parts = urlsplit(uri)
    query = parse_qs(parts.query)
    if parts.scheme.lower().startswith("sqlite"):
        database = parts.path[1:]  # sqlite:///relative.db or sqlite:////absolute.db
        if not database or ":memory:" in database or "memory" in query.get("mode", []):
            return unsupported("in-memory SQLite catalogs are not persistent")
    elif not parts.hostname:
        # SQLAlchemy also takes the server from a "host" query parameter, possibly a
        # socket directory or several hosts. Only a single network host is identifiable.
        hosts = query.get("host", [])
        if len(hosts) != 1 or not hosts[0] or hosts[0].startswith("/") or "," in hosts[0]:
            return unsupported("the SQL catalog uri does not name a single database server")
        host = hosts[0].strip("[]")
        if ":" in host:  # IPv6 literal
            host = f"[{host}]"
        port = query.get("port", [""])[0]
        parts = parts._replace(netloc=f"{host}:{port}" if port else host)
    namespace = _strip_credentials(parts, drop_driver=True)
    return PolarsCatalog(alias=alias, namespace=namespace, catalog=alias)


def _normalize_warehouse(warehouse: Any) -> str:
    warehouse = str(warehouse).strip()
    if "://" in warehouse:
        # Some REST services take a storage location as warehouse.
        return _strip_credentials(urlsplit(warehouse))
    return warehouse


def _strip_credentials(parts: SplitResult, drop_driver: bool = False) -> str:
    """Rebuild a URL without user info, query string or fragment.

    The scheme and host are lowercased, a default HTTP(S) port and a trailing slash are
    removed. ``drop_driver`` removes a SQLAlchemy driver suffix (``postgresql+psycopg2``).
    """
    scheme = parts.scheme.lower()
    if drop_driver:
        scheme = scheme.split("+", 1)[0]
    host = parts.hostname or ""
    if ":" in host:
        host = f"[{host}]"
    try:
        port = parts.port
    except ValueError:
        port = None
    if port is not None and port != _DEFAULT_PORTS.get(scheme):
        host = f"{host}:{port}"
    path = parts.path
    if scheme in _DEFAULT_PORTS:
        path = path.rstrip("/")
    return f"{scheme}://{host}{path}"
