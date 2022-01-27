#!/bin/bash
#
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
#
# Usage: $ ./entrypoint.sh

set -e

if [[ -z "${OPENLINEAGE_PROXY_CONFIG}" ]]; then
  OPENLINEAGE_PROXY_CONFIG='proxy.dev.yml'
  echo "WARNING 'OPENLINEAGE_PROXY_CONFIG' not set, using development configuration."
fi

# Start http server with configuration
java -Duser.timezone=UTC -Dlog4j2.formatMsgNoLookups=true -jar openlineage-proxy-*.jar server "${OPENLINEAGE_PROXY_CONFIG}"