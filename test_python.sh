#!/usr/bin/env bash

pip install maturin
maturin build
pip install target/wheels/*.whl
pip install pytest

pytest -vv tests/python
