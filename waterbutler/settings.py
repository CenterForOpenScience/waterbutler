import os
import json
import logging
import logging.config


class SettingsDict(dict):

    def __init__(self, *args, parent=None, **kwargs):
        self.parent = parent
        super().__init__(*args, **kwargs)

    def get(self, key, default=None):
        env = '{}_{}'.format(self.parent, key) if self.parent else key
        if env in os.environ:
            return os.environ.get(env)
        return super().get(key, default)

    def child(self, key):
        env = '{}_{}'.format(self.parent, key) if self.parent else key
        return SettingsDict(self.get(key, {}), parent=env)


PROJECT_NAME = 'waterbutler'
PROJECT_CONFIG_PATH = '~/.cos'

try:
    import colorlog  # noqa
    DEFAULT_FORMATTER = {
        '()': 'colorlog.ColoredFormatter',
        'format': '%(cyan)s[%(asctime)s]%(log_color)s[%(levelname)s][%(name)s]: %(reset)s%(message)s'
    }
except ImportError:
    DEFAULT_FORMATTER = {
        '()': 'waterbutler.core.logging.MaskFormatter',
        'format': '[%(asctime)s][%(levelname)s][%(name)s]: %(message)s',
        'pattern': '(?<=cookie=)(.*?)(?=&|$)',
        'mask': '***'
    }
DEFAULT_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'console': DEFAULT_FORMATTER,
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'console'
        },
        'syslog': {
            'class': 'logging.handlers.SysLogHandler',
            'level': 'INFO'
        }
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console']
    }
}


try:
    config_path = os.environ['{}_CONFIG'.format(PROJECT_NAME.upper())]
except KeyError:
    env = os.environ.get('ENV', 'test')
    config_path = '{}/{}-{}.json'.format(PROJECT_CONFIG_PATH, PROJECT_NAME, env)


config = SettingsDict()
config_path = os.path.expanduser(config_path)
if not os.path.exists(config_path):
    logging.warning('No \'{}\' configuration file found'.format(config_path))
else:
    with open(os.path.expanduser(config_path)) as fp:
        config = SettingsDict(json.load(fp))


def child(key):
    return config.child(key)


DEBUG = config.get('DEBUG', True)
REQUEST_LIMIT = config.get('REQUEST_LIMIT', 10)
OP_CONCURRENCY = config.get('OP_CONCURRENCY', 5)

logging_config = config.get('LOGGING', DEFAULT_LOGGING_CONFIG)
logging.config.dictConfig(logging_config)

SENTRY_DSN = config.get('SENTRY_DSN', None)

analytics_config = config.child('ANALYTICS')
MFR_IDENTIFYING_HEADER = analytics_config.get('MFR_IDENTIFYING_HEADER', 'X-Cos-Mfr-Render-Request')
MFR_DOMAIN = analytics_config.get('MFR_DOMAIN', 'http://localhost:7778').rstrip('/')

keen_config = analytics_config.child('KEEN')
KEEN_API_BASE_URL = keen_config.get('API_BASE_URL', 'https://api.keen.io')
KEEN_API_VERSION = keen_config.get('API_VERSION', '3.0')

keen_private_config = keen_config.child('PRIVATE')
KEEN_PRIVATE_PROJECT_ID = keen_private_config.get('PROJECT_ID', None)
KEEN_PRIVATE_WRITE_KEY = keen_private_config.get('WRITE_KEY', None)

keen_public_config = keen_config.child('PUBLIC')
KEEN_PUBLIC_PROJECT_ID = keen_public_config.get('PROJECT_ID', None)
KEEN_PUBLIC_WRITE_KEY = keen_public_config.get('WRITE_KEY', None)
