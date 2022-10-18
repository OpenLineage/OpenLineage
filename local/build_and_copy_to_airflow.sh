#!/usr/bin/env bash

set -e

mkdir -p ~/code/airflow/files/ol

rm -r client/python/dist/*
rm -r integration/common/dist/*
rm -r integration/airflow/dist/*

(cd client/python && python setup.py bdist_wheel)
(cd integration/common && python setup.py bdist_wheel)
(cd integration/airflow && python setup.py bdist_wheel)

cp client/python/dist/* ~/code/airflow/files/ol
cp integration/common/dist/* ~/code/airflow/files/ol
cp integration/airflow/dist/* ~/code/airflow/files/ol
