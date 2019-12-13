import json
import subprocess
from subprocess import CompletedProcess

from flask import Flask, Response

app = Flask(__name__)
# spark_submit = "spark-submit"
mime_json = "application/json"


def execute_driver(name: str, version: int) -> CompletedProcess:
    return subprocess.run(["spark-submit",
                           "--py-files", "/jobs/dist/" + name + "-" + str(version) + "-py3.7.egg",
                           "/jobs/" + name + "/driver.py"])


@app.route("/ingest/streaming", methods=["POST"])
def ingest_streaming():
    # dataset = request.json["dataset"]  # TODO
    result = execute_driver("data_importer", 1)

    response_body = json.dumps({"code": result.returncode})
    response_code = result.returncode == 0 if 201 else 500

    return Response(response_body,
                    status=response_code,
                    mimetype=mime_json)


if __name__ == "__main__":
    # For cluster
    app.run(host="0.0.0.0", port=8080)

    # For local testing
    # app.run(port=8080)
