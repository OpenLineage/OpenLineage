#!/usr/bin/env bash
awslocal s3 mb s3://testbucket
echo "OpenLineage rocks!" > testfile
awslocal s3 cp testfile s3://testbucket/
rm testfile