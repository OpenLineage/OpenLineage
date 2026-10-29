# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""OAuth2 refresh-token authentication (acts on behalf of a named user).

For an SAP Datasphere OAuth client with purpose "Interactive Usage", whose documented flow is the
3-legged authorization_code grant. The browser step happens once, with ``python -m app.oauth_login``,
which prints a refresh token. This provider exchanges that refresh token for access tokens, caching
them like the client-credentials provider.

If the token endpoint rotates the refresh token, the new one is used for the rest of the process. The
stored secret is then stale for the next start, which is logged (without the token value).
"""

from __future__ import annotations

import logging

from app.auth._oauth2 import _OAuth2TokenAuth

log = logging.getLogger(__name__)


class OAuth2RefreshTokenAuth(_OAuth2TokenAuth):
    def __init__(self, token_url: str, client_id: str, client_secret: str, refresh_token: str, **kwargs) -> None:
        super().__init__(token_url, client_id, client_secret, **kwargs)
        self._refresh_token = refresh_token

    def _grant(self) -> dict[str, str]:
        return {"grant_type": "refresh_token", "refresh_token": self._refresh_token}

    def _on_token_response(self, payload: dict) -> None:
        rotated = payload.get("refresh_token")
        if isinstance(rotated, str) and rotated and rotated != self._refresh_token:
            self._refresh_token = rotated
            log.warning(
                "The token endpoint rotated the refresh token. The new one is used until this process "
                "exits; run `python -m app.oauth_login` again and update DATASPHERE_REFRESH_TOKEN before "
                "the stored value expires."
            )
