#!/bin/bash
#
# SPDX-License-Identifier: Apache-2.0
#
# Usage: $ ./get-docker-compose.sh

set -e

curl -L https://github.com/docker/compose/releases/download/1.25.3/docker-compose-`uname -s`-`uname -m` > ~/docker-compose
chmod +x ~/docker-compose
sudo mv ~/docker-compose /usr/local/bin/docker-compose
docker-compose --version

echo "DONE!"