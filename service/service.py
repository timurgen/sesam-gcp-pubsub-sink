import os
import logging
import json

from flask import Flask, request, Response
from google.cloud import pubsub_v1


APP = Flask(__name__)

PROJECT_ID = os.environ.get('PROJECT_ID')


CREDENTIALS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_CONTENT")

log_level = logging.getLevelName(os.environ.get("LOG_LEVEL", "INFO"))
logging.basicConfig(level=log_level)

if CREDENTIALS:
    with open(CREDENTIALS_PATH, "wb") as out_file:
        out_file.write(CREDENTIALS.encode())


@APP.route("/<topic_name>", methods=['POST'])
def process(topic_name):
    """
    Endpoint to publish messages to GCP pubsub
    :param topic_name: name of topic to publish messages to
    :return:
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, topic_name)
    input_data = request.get_json()

    def generate():
        yield "["
        for index, input_entity in enumerate(input_data):
            output_entity = dict()
            output_entity['_id'] = input_entity['_id']
            if index > 0:
                yield ","
            data = json.dumps(input_entity).encode("utf-8")
            future = publisher.publish(topic_path, data=data)
            try:
                output_entity['result'] = future.result()
            except Exception as e:
                output_entity['result'] = e
            yield json.dumps(output_entity)
        yield "]"
    return Response(generate(), content_type="application/json")


if __name__ == "__main__":
    APP.run(threaded=True, debug=True, host='0.0.0.0', port=os.environ.get('PORT', 5000))
