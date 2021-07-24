import logging
import json

from waterbutler.providers.onedrive import OneDriveProvider

logger = logging.getLogger(__name__)


class OneDriveBusinessProvider(OneDriveProvider):

    NAME = 'onedrivebusiness'

    def __init__(self, auth, credentials, settings, **kwargs):
        super().__init__(auth, credentials, settings, **kwargs)
        #self.admin_dbmid = self.settings['admin_dbmid']
        #self.team_folder_id = self.settings['team_folder_id']
        logger.info('settings: {}'.format(settings))
