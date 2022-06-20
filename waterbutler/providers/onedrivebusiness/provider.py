import logging

from waterbutler.core import provider
from waterbutler.providers.onedrive import OneDriveProvider
from waterbutler.providers.onedrive import settings

logger = logging.getLogger(__name__)


class OneDriveBusinessProvider(OneDriveProvider):

    NAME = 'onedrivebusiness'

    def __init__(self, auth, credentials, settings, **kwargs):
        super().__init__(auth, credentials, settings, **kwargs)
        logger.info('settings: {}'.format(settings))
        self.drive_id = settings['drive_id']

    def _build_drive_url(self, *segments, **query) -> str:
        base_url = settings.BASE_URL
        if self.drive_id is None:
            return provider.build_url(base_url, 'drive', *segments, **query)
        else:
            return provider.build_url(base_url, 'drives', self.drive_id, *segments, **query)

    def _build_item_url(self, *segments, **query) -> str:
        base_url = settings.BASE_URL
        if self.drive_id is None:
            return provider.build_url(base_url, 'drive', 'items', *segments, **query)
        else:
            return provider.build_url(base_url, 'drives', self.drive_id, 'items', *segments, **query)
