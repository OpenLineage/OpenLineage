#!/bin/bash
# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

#
# SPDX-License-Identifier: Apache-2.0.
#
# Usage: $ ./get-docker-compose.sh

set -e

curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" > ~/docker-compose
chmod +x ~/docker-compose
sudo mv ~/docker-compose /usr/local/bin/docker-compose
docker-compose --version

echo "DONE!"
