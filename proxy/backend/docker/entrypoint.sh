#!/bin/bash
#
# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
#
# Usage: $ ./entrypoint.sh

set -e

if [[ -z "${OPENLINEAGE_PROXY_CONFIG}" ]]; then
  OPENLINEAGE_PROXY_CONFIG='proxy.dev.yml'
  echo "WARNING 'OPENLINEAGE_PROXY_CONFIG' not set, using development configuration."
fi

# Adjust java options for the http server
JAVA_OPTS="${JAVA_OPTS} -Duser.timezone=UTC -Dlog4j2.formatMsgNoLookups=true"

# Start http server with java options (if any) and configuration
java ${JAVA_OPTS} -jar openlineage-proxy-*.jar server ${OPENLINEAGE_PROXY_CONFIG}
