# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""OAuth2 client-credentials authentication with in-memory token caching.

For a technical user (2-legged): an SAP Datasphere OAuth client with purpose "API Access".
Client-credentials issues no refresh token, so we simply re-POST the token endpoint when the cached
token nears expiry or a request comes back 401.
"""

from __future__ import annotations

from app.auth._oauth2 import _OAuth2TokenAuth


class OAuth2ClientCredentialsAuth(_OAuth2TokenAuth):
    def _grant(self) -> dict[str, str]:
        return {"grant_type": "client_credentials"}
