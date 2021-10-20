import logging
import sys

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
Simple service that does receive POST requests with OpenLineage events
and, upon /check call, compares them with expected events stored in 
file that was provided during build process.

Events are matched by job name and event type.

The comparison omits fields that are not in event stored in provided file. 
"""


def get_conn():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.execute('''
            CREATE TABLE IF NOT EXISTS requests (body text, job_name text, created_at text)
        ''')
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


@app.route("/api/v1/lineage", methods=['GET', 'POST'])
def lineage():
    conn = get_conn()
    if request.method == 'POST':
        job_name = request.json['job']['name']
        conn.execute("""
            INSERT INTO requests values (:body, :job_name, CURRENT_TIMESTAMP)
        """, {
            "body": json.dumps(request.json),
            "job_name": job_name
        })
        logger.info(f"job_name: {job_name}")
        logger.info(json.dumps(request.json, sort_keys=True))
        conn.commit()
        return '', 200
    elif request.method == 'GET':
        job_name = request.args.get("job_name")
        if job_name:
            received_requests = conn.execute("""
                SELECT body FROM requests WHERE job_name LIKE :job_name ORDER BY created_at
            """, {
                "job_name": f'{job_name}%'
            }).fetchall()
        else:
            received_requests = conn.execute("""
                SELECT body FROM requests
            """).fetchall()
        received_requests = [json.loads(req[0]) for req in received_requests]

        logger.info(f"GOT {len(received_requests)} requests for job {job_name}")

        return jsonify(received_requests), 200
