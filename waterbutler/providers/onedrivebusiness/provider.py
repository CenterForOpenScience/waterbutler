import logging

from waterbutler.providers.onedrive import OneDriveProvider

logger = logging.getLogger(__name__)


class OneDriveBusinessProvider(OneDriveProvider):

    NAME = 'onedrivebusiness'

    def __init__(self, auth, credentials, settings, **kwargs):
        super().__init__(auth, credentials, settings, **kwargs)
        logger.info('settings: {}'.format(settings))
