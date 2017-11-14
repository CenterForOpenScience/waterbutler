import os

from pkg_resources import iter_entry_points
from kombu import Queue, Exchange

from waterbutler import settings


config = settings.child('TASKS_CONFIG')

BROKER_URL = config.get(
    'BROKER_URL',
    'amqp://{}:{}//'.format(
        os.environ.get('RABBITMQ_PORT_5672_TCP_ADDR', ''),
        os.environ.get('RABBITMQ_PORT_5672_TCP_PORT', ''),
    )
)
CELERY_RESULT_BACKEND = config.get(
    'CELERY_RESULT_BACKEND',
    '{}://{}:{}/{}'.format(
        os.environ.get('CELERY_RESULT_BACKEND_PROTO', ''),
        os.environ.get('CELERY_RESULT_BACKEND_TCP_ADDR', ''),
        os.environ.get('CELERY_RESULT_BACKEND_TCP_PORT', ''),
        os.environ.get('CELERY_RESULT_BACKEND_ID', ''),
    )
)

WAIT_TIMEOUT = int(config.get('WAIT_TIMEOUT', 15))
# For testing 202 response
# WAIT_TIMEOUT = int(config.get('WAIT_TIMEOUT', 1))
WAIT_INTERVAL = float(config.get('WAIT_INTERVAL', 0.5))
ADHOC_BACKEND_PATH = config.get('ADHOC_BACKEND_PATH', '/tmp')

CELERY_CREATE_MISSING_QUEUES = config.get_bool('CELERY_CREATE_MISSING_QUEUES', False)
CELERY_DEFAULT_QUEUE = config.get('CELERY_DEFAULT_QUEUE', 'waterbutler')
CELERY_QUEUES = (
    Queue('waterbutler', Exchange('waterbutler'), routing_key='waterbutler'),
)
CELERY_IMPORTS = [
    entry.module_name
    for entry in iter_entry_points(group='waterbutler.providers.tasks', name=None)
]
CELERY_IMPORTS.extend([
    'waterbutler.tasks.move'
])

CELERY_ACKS_LATE = True
CELERYD_HIJACK_ROOT_LOGGER = False
