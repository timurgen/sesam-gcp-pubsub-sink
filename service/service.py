import os
import logging
import json

from string_utils import str_to_bool
from flask import Flask, request, Response, abort
from google.cloud import pubsub_v1

APP = Flask(__name__)

PROJECT_ID = os.environ.get('PROJECT_ID')
PAYLOAD_KEY = os.environ.get('PAYLOAD_KEY')

CREDENTIALS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_CONTENT")
FAIL_ON_ERROR = str_to_bool(os.environ.get('FILE_ON_ERROR', "True"))

THREAD_POOL_SIZE = int(os.environ.get('THREAD_POOL_SIZE', '10'))

log_level = logging.getLevelName(os.environ.get("LOG_LEVEL", "INFO"))
logging.basicConfig(level=log_level)

if not PROJECT_ID:
    logging.error("Google cloud platform project id is undefined")

logging.info("Project id: {}".format(PROJECT_ID))
logging.info("Payload entity key: {}".format(PAYLOAD_KEY))

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

        def callback(future):
            if index > 0:
                yield ","
            output_entity['result'] = future.result()
            yield json.dumps(output_entity)

        yield "["
        for index, input_entity in enumerate(input_data):
            output_entity = dict()
            output_entity['_id'] = input_entity['_id']

            data: str = json.dumps(input_entity[PAYLOAD_KEY] if PAYLOAD_KEY else input_entity).encode("utf-8")
            logging.debug("data to be sent: {}".format(data))
            try:
                input_entity['future'] = publisher.publish(topic_path, data=data)
                input_entity['future'].add_done_callback(callback)
            except Exception as e:
                logging.error(e)
                if FAIL_ON_ERROR:
                    abort(500, str(e))
                output_entity['result'] = "ERROR: {}".format(str(e))
        yield "]"

    return Response(generate(), content_type="application/json")


if __name__ == "__main__":
    port = int(os.environ.get('PORT', '5000'))
    if logging.isEnabledFor(logging.DEBUG):
        APP.run(debug=True, host='0.0.0.0', port=port)
    else:
        import cherrypy

        cherrypy.tree.graft(APP, '/')
        cherrypy.config.update({
            'environment': 'production',
            'engine.autoreload_on': True,
            'log.screen': False,
            'server.socket_port': port,
            'server.socket_host': '0.0.0.0',
            'server.thread_pool': THREAD_POOL_SIZE,
            'server.max_request_body_size': 0
        })

        cherrypy.engine.start()
        cherrypy.engine.block()
