#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0

SPEC_DIR="$(pwd)/static/spec"
APIDOC_DIR="$(pwd)/static/apidocs"

pushd $SPEC_DIR
LATEST_VERSION=$(find . -maxdepth 1 | grep -v 'facets' | grep '[0-9]*-[0-9]-[0-9]' | sort -Vr | head -1)
echo latest version is $LATEST_VERSION
rm ./OpenLineage.json 2>/dev/null
perl -i -pe"s/version: [[:alnum:]\.-]*/version: ${LATEST_VERSION:2}/g" ./OpenLineage.yml

mkdir "${LATEST_VERSION}/facets"
for i in $(ls -d ./facets/* | sort); do cp $i/*.json ${LATEST_VERSION}/facets; done;

pushd $LATEST_VERSION
ln -sf ../OpenLineage.yml .
yarn run redoc-cli build --output "${APIDOC_DIR}/openapi/index.html" "${SPEC_DIR}/${LATEST_VERSION}/OpenLineage.yml" --title 'OpenLineage API Docs'
rm -rf facets
rm OpenLineage.yml
popd

popd
