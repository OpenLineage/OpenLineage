# SPDX-License-Identifier: Apache-2.0

import logging
import sys
import time

from flask import Flask, request, g, jsonify
import sqlite3
import json

app = Flask(__name__)

DATABASE = "/app/tmp.db"

logging.basicConfig(
    format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    level='DEBUG'
)
logger = logging.getLogger(__name__)


"""
Simple service that fails in different way to check if integration behavior
works as desired
"""


@app.route("/error/api/v1/lineage", methods=['GET', 'POST'])
def error_lineage():
    logger.warning("Called error endpoint")
    return "", 500


@app.route("/timeout/api/v1/lineage", methods=['GET', 'POST'])
def timeout_lineage():
    logger.warning("Called timeout endpoint")
    time.sleep(15)
    return jsonify({}), 200

