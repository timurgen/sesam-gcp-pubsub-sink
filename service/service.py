import os
import logging
import json

import google
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

SUBSCRIPTION_BATCH_SIZE = int(os.environ.get('SUBSCRIPTION_MAX_SIZE', '1_000'))

log_level = logging.getLevelName(os.environ.get("LOG_LEVEL", "INFO"))
logging.basicConfig(level=log_level)

LOG = logging.getLogger(__name__)

if not PROJECT_ID:
    LOG.error("Google Cloud Platform project id is undefined")
    exit(1)

LOG.info(f"project id: {PROJECT_ID}")
LOG.info(f"payload entity key: {PAYLOAD_KEY}")

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
    LOG.info(f'serving publisher request for topic {topic_path}')
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
            LOG.debug(f"data to be sent: {data}")
            try:
                input_entity['future'] = publisher.publish(topic_path, data=data)
                input_entity['future'].add_done_callback(callback)
            except Exception as e:
                LOG.error(e)
                if FAIL_ON_ERROR:
                    abort(500, str(e))
                output_entity['result'] = f"ERROR: {e}"
        yield "]"

    return Response(generate(), content_type="application/json")


@APP.route('/<subscription_name>', methods=['GET'])
def consume(subscription_name):
    def generate(messages):
        first = True
        yield '['

        for msg in messages.received_messages:
            data_item = json.loads(msg.message.data)
            if type(data_item) is list:
                data_item = {'data': data_item}
            data_item['_id'] = str(msg.message.message_id)
            data_item['_updated'] = msg.message.publish_time.seconds
            if not first:
                yield ','
            else:
                first = False
            yield json.dumps(data_item)
        yield ']'
        LOG.debug(f'{len(messages.received_messages)}  messages processed')
        ack_ids = [msg.ack_id for msg in messages.received_messages]

        if ack_ids:
            subscriber.acknowledge(sub_path, ack_ids)
            LOG.debug(f'{len(ack_ids)} acknowledged')
        subscriber.close()

    subscriber = pubsub_v1.SubscriberClient()

    sub_path = subscriber.subscription_path(PROJECT_ID, subscription_name)
    LOG.info(f'serving consumer request to topic {subscription_name} for subscription {sub_path}')
    with subscriber:
        try:
            response = subscriber.pull(sub_path, max_messages=SUBSCRIPTION_BATCH_SIZE, return_immediately=True)
        except google.api_core.exceptions.DeadlineExceeded as e:
            LOG.warning(str(e))
        else:
            return Response(generate(response), content_type='application/json')
        LOG.debug("request didn't return any result")
        return Response(json.dumps([]), content_type='application/json')


if __name__ == "__main__":
    port = int(os.environ.get('PORT', '5000'))
    if logging.getLogger().isEnabledFor(logging.DEBUG):
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
