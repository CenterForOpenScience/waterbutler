import os
import json
import logging
import logging.config


class SettingsDict(dict):
    """Allow overriding on-disk config via environment variables.  Normal config is done with a
    hierarchical dict::

        "SERVER_CONFIG": {
          "HOST": "http://localhost:7777"
        }

    ``HOST`` can be retrieved in the python code with::

        config = SettingsDict(json.load('local-config.json'))
        server_cfg = config.child('SERVER_CONFIG')
        host = server_cfg.get('HOST')

    To override a value, join all of the parent keys and the child keys with an underscore::

        $ SERVER_CONFIG_HOST='http://foo.bar.com' invoke server

    Nested dicts can be handled with the ``.child()`` method.  Config keys will be all parent keys
    joined by underscores::

        "SERVER_CONFIG": {
          "ANALYTICS": {
            "PROJECT_ID": "foo"
          }
        }

    The corresponding envvar for ``PROJECT_ID`` would be ``SERVER_CONFIG_ANALYTICS_PROJECT_ID``.
    """

    def __init__(self, *args, parent=None, **kwargs):
        self.parent = parent
        super().__init__(*args, **kwargs)

    def get(self, key, default=None):
        """Fetch a config value for ``key`` from the settings.  First checks the env, then the
        on-disk config.  If neither exists, returns ``default``."""
        env = self.full_key(key)
        if env in os.environ:
            return os.environ.get(env)
        return super().get(key, default)

    def get_bool(self, key, default=None):
        """Fetch a config value and interpret as a bool. Since envvars are always strings,
        interpret '0' and the empty string as False and '1' as True.  Anything else is probably
        an acceident, so die screaming."""
        value = self.get(key, default)
        if value in [False, 0, '0', '']:
            retval = False
        elif value in [True, 1, '1']:
            retval = True
        else:
            raise Exception(
                '{} should be a truthy value, but instead we got {}'.format(
                    self.full_key(key), value
                )
            )
        return retval

    def get_nullable(self, key, default=None):
        """Fetch a config value and interpret the empty string as None. Useful for external code
        that expects an explicit None."""
        value = self.get(key, default)
        return None if value == '' else value

    def get_object(self, key, default=None):
        """Fetch a config value and interpret as a Python object or list. Since envvars are
        always strings, interpret values of type `str` as JSON object or array. Otherwise assume
        the type is already a python object."""
        value = self.get(key, default)
        if isinstance(value, str):
            value = json.loads(value)
        return value

    def full_key(self, key):
        """The name of the envvar which corresponds to this key."""
        return '{}_{}'.format(self.parent, key) if self.parent else key

    def child(self, key):
        """Fetch a sub-dict of the current dict."""
        return SettingsDict(self.get(key, {}), parent=self.full_key(key))


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


DEBUG = config.get_bool('DEBUG', True)
OP_CONCURRENCY = int(config.get('OP_CONCURRENCY', 5))

logging_config = config.get('LOGGING', DEFAULT_LOGGING_CONFIG)
logging.config.dictConfig(logging_config)

SENTRY_DSN = config.get_nullable('SENTRY_DSN', None)

analytics_config = config.child('ANALYTICS')
MFR_IDENTIFYING_HEADER = analytics_config.get('MFR_IDENTIFYING_HEADER', 'X-Cos-Mfr-Render-Request')
MFR_DOMAIN = analytics_config.get('MFR_DOMAIN', 'http://localhost:7778').rstrip('/')

keen_config = analytics_config.child('KEEN')
KEEN_API_BASE_URL = keen_config.get('API_BASE_URL', 'https://api.keen.io')
KEEN_API_VERSION = keen_config.get('API_VERSION', '3.0')
KEEN_ENABLE_LOGGING = keen_config.get_bool('ENABLE_LOGGING', True)

keen_private_config = keen_config.child('PRIVATE')
KEEN_PRIVATE_PROJECT_ID = keen_private_config.get_nullable('PROJECT_ID', None)
KEEN_PRIVATE_WRITE_KEY = keen_private_config.get_nullable('WRITE_KEY', None)
KEEN_PRIVATE_LOG_ACTIONS = keen_private_config.get_bool('LOG_ACTIONS', True)

keen_public_config = keen_config.child('PUBLIC')
KEEN_PUBLIC_PROJECT_ID = keen_public_config.get_nullable('PROJECT_ID', None)
KEEN_PUBLIC_WRITE_KEY = keen_public_config.get_nullable('WRITE_KEY', None)
KEEN_PUBLIC_LOG_ACTIONS = keen_public_config.get_bool('LOG_ACTIONS', True)

WEBDAV_METHODS = {'PROPFIND', 'MKCOL', 'MOVE', 'COPY'}

AIOHTTP_TIMEOUT = int(config.get('AIOHTTP_TIMEOUT', 3600))  # time in seconds
