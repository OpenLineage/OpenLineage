#!/usr/bin/env bash

pip install maturin
maturin build
pip install target/wheels/*.whl
pip install pytest

pytest -vv tests/python


#docker run --rm -it --platform linux/arm64/v8 --entrypoint bash -v $PWD:/code $(docker build -q .)