#!/bin/bash

set -e

PMD_RELEASE="https://github.com/pmd/pmd/releases/download/pmd_releases%2F6.46.0/pmd-bin-6.46.0.zip"

cd /opt/.pmd_cache

# check if there is a pmd folder
if [ ! -d "pmd" ]; then
  wget -nc -O pmd.zip "$PMD_RELEASE" > /dev/null 2>&1 \
    && unzip pmd.zip > /dev/null 2>&1 \
    && rm pmd.zip > /dev/null 2>&1 \
    && mv pmd-bin* pmd > /dev/null 2>&1 \
    && chmod -R +x pmd > /dev/null 2>&1
fi

idx=1
for (( i=1; i <= "$#"; i++ )); do
    if [[ ${!i} == *.java ]]; then
        idx=${i}
        break
    fi
done

# add default ruleset if not specified
if [[ ! $pc_args == *"-R "* ]]; then
  pc_args="$pc_args -R /src/client/java/pmd-openlineage.xml"
fi

# populate list of files to analyse
files=""
prefix="/src/"
for arg in "${@:idx}"; do
  files="$files $prefix$arg"
done

# Remove leading space (optional)
files="${files:1}"
eol=$'\n'
echo "${files// /$eol}" > /tmp/list

./pmd/bin/run.sh pmd -f textcolor -min 5 --file-list /tmp/list $pc_args