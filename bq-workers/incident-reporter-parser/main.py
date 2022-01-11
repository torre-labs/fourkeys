# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import os
import json

import shared

from flask import Flask, request

app = Flask(__name__)


@app.route("/", methods=["POST"])
def index():
    """
    Receives messages from a push subscription from Pub/Sub.
    Parses the message, and inserts it into BigQuery.
    """
    event = None
    envelope = request.get_json()

    # Check that data has been posted
    if not envelope:
        raise Exception("Expecting JSON payload")
    # Check that message is a valid pub/sub message
    if "message" not in envelope:
        raise Exception("Not a valid Pub/Sub Message")
    msg = envelope["message"]

    if "attributes" not in msg:
        raise Exception("Missing pubsub attributes")

    try:
        event = process_new_source_event(msg)

        # [Do not edit below]
        shared.insert_row_into_bigquery(event)

    except Exception as e:
        print(e)
        entry = {
            "severity": "WARNING",
            "msg": "Data not saved to BigQuery",
            "errors": str(e),
            "json_payload": envelope
        }
        print(json.dumps(entry))

    return "", 204


def process_new_source_event(msg):
    signature = shared.create_unique_id(msg)

    metadata = json.loads(base64.b64decode(msg["data"]).decode("utf-8").strip())

    new_source_event = {
        "event_type": "incident",
        "id": "notion-%s" % metadata["id"],
        "metadata": json.dumps(metadata),
        "time_created": metadata["created_time"],
        "signature": signature,
        "msg_id": msg["message_id"],
        "source": "incident-reporter",
    }

    print(new_source_event)
    return new_source_event


if __name__ == "__main__":
    PORT = int(os.getenv("PORT")) if os.getenv("PORT") else 8080

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    app.run(host="127.0.0.1", port=PORT, debug=True)
