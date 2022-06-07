import attr

from typing import Optional

from openlineage.client.facet import BaseFacet
from openlineage.common.schema import GITHUB_LOCATION


@attr.s
class LiveMapsPythonDecoratedFacet(BaseFacet):
    """
    Facet that represents metadata relevant to LiveMaps Graph UI.
    :param database: The database type/name
    :param cluster: Cluster of the cloud database(Optional)
    :param connectionUrl: Database connection URL
    :param target: Target of Link/outEdge for LiveMaps Graph UI.
    :param source: Source of link/inEdge for LiveMaps Graph UI.
    """

    database: Optional[str] = attr.ib(default=None)
    cluster: Optional[str] = attr.ib(default=None)
    connectionUrl: Optional[str] = attr.ib(default=None)
    target: Optional[str] = attr.ib(default=None)
    source: Optional[str] = attr.ib(default=None)

    @staticmethod
    def _get_schema() -> str:
        return GITHUB_LOCATION + "live-maps-python-decorated-facet.json"
