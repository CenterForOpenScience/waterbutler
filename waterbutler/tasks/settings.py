import os

from pkg_resources import iter_entry_points
from kombu import Queue, Exchange

from waterbutler import settings


config = settings.child('TASKS_CONFIG')

WAIT_TIMEOUT = int(config.get('WAIT_TIMEOUT', 20))
WAIT_INTERVAL = float(config.get('WAIT_INTERVAL', 0.5))
ADHOC_BACKEND_PATH = config.get('ADHOC_BACKEND_PATH', '/tmp')

broker_url = config.get(
    'BROKER_URL',
    'amqp://{}:{}//'.format(
        os.environ.get('RABBITMQ_PORT_5672_TCP_ADDR', ''),
        os.environ.get('RABBITMQ_PORT_5672_TCP_PORT', ''),
    )
)

task_default_queue = config.get('CELERY_DEFAULT_QUEUE', 'waterbutler')
task_queues = (
    Queue('waterbutler', Exchange('waterbutler'), routing_key='waterbutler'),
)

task_always_eager = config.get_bool('CELERY_ALWAYS_EAGER', False)
result_backend = config.get_nullable('CELERY_RESULT_BACKEND', 'rpc://')
result_persistent = config.get_bool('CELERY_RESULT_PERSISTENT', True)
worker_disable_rate_limits = config.get_bool('CELERY_DISABLE_RATE_LIMITS', True)
result_expires = int(config.get('CELERY_TASK_RESULT_EXPIRES', 60))
task_create_missing_queues = config.get_bool('CELERY_CREATE_MISSING_QUEUES', False)
task_acks_late = True
worker_hijack_root_logger = False
task_eager_propagates = True

imports = [
    entry.module_name
    for entry in iter_entry_points(group='waterbutler.providers.tasks', name=None)
]
imports.extend([
    'waterbutler.tasks.move'
])
