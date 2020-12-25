import json

from waterbutler.providers.dropbox import DropboxProvider


class DropboxBusinessProvider(DropboxProvider):

    NAME = 'dropboxbusiness'

    def __init__(self, auth, credentials, settings, **kwargs):
        super().__init__(auth, credentials, settings, **kwargs)
        self.admin_dbmid = self.settings['admin_dbmid']
        self.team_folder_id = self.settings['team_folder_id']

    @property
    def default_headers(self) -> dict:
        return dict(super().default_headers, **{
            'Dropbox-API-Select-Admin': self.admin_dbmid,
            'Dropbox-API-Path-Root': json.dumps({
                '.tag': 'namespace_id',
                'namespace_id': self.team_folder_id,
            })
        })
