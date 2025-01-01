#!/usr/bin/env bash
# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

awslocal s3 mb s3://testbucket
echo "OpenLineage rocks!" > testfile
awslocal s3 cp testfile s3://testbucket/
rm testfile