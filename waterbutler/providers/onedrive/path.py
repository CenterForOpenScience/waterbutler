from itertools import repeat
from urllib.parse import urlparse

from waterbutler.core.path import WaterButlerPath


class OneDrivePath(WaterButlerPath):
    """OneDrive specific WaterButlerPath class to handle some of the idiosyncrasies of
    file paths in OneDrive."""

    def file_path(self, data):
        parent_path = data['parentReference']['path'].replace('/drive/root:', '')
        if (len(parent_path) == 0):
            names = '/{}'.format(data['name'])
        else:
            names = '{}/{}'.format(parent_path, data['name'])
        return names

    def ids(self, data):
        ids = [data['parentReference']['id'], data['id']]
        url_segment_count = len(urlparse(self.file_path(data)).path.split('/'))
        if (len(ids) < url_segment_count):
            for x in repeat(None, url_segment_count - len(ids)):
                ids.insert(0, x)
        return ids

    def one_drive_parent_folder(self, path):
        if path._prepend == '0':
            folder = 'drive/root:/'
        elif str(path.parent) == '/':
            folder = path._prepend
        else:
            folder = path.path.replace(path.name, '')
        return folder

    def parent_id(self):
        if self.parent.identifier is None:
            return self.parent.full_path.strip('/')

        return self.parent.identifier

    def api_identifier(self):
        return self.identifier
