#!/bin/bash

set -euxo pipefail

readonly VM_HADOOP_LIB_DIR=/usr/lib/hadoop/lib
readonly VM_DATAPROC_VM_HADOOP_LIB_DIR_DIR=/usr/local/share/google/dataproc/lib
readonly JAR_URLS=$(/usr/share/google/get_metadata_value attributes/jar-urls || true)

if [[ -d ${VM_DATAPROC_VM_HADOOP_LIB_DIR_DIR} ]]; then
  vm_lib_dir=${VM_DATAPROC_VM_HADOOP_LIB_DIR_DIR}
else
  vm_lib_dir=${VM_HADOOP_LIB_DIR}
fi

IFS=',' read -ra URLS <<< "$JAR_URLS"
for url in "${URLS[@]}"; do
    gsutil cp -P "$url" "$vm_lib_dir/"
    if [ $? -eq 0 ]; then
        echo "Successfully copied $url to $vm_lib_dir/"
    else
        echo "Failed to copy $url to $vm_lib_dir/"
    fi
done
