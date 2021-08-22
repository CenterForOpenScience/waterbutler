import logging
from itertools import repeat
from urllib import parse as urlparse

from waterbutler.core.path import WaterButlerPath

logger = logging.getLogger(__name__)


class OneDrivePath(WaterButlerPath):
    """OneDrive specific WaterButlerPath class to handle some of the idiosyncrasies of
    file paths in OneDrive."""

    @classmethod
    def new_from_response(cls, response, base_folder_id, base_folder_metadata=None):
        """Build a new `OneDrivePath` object from a OneDrive API response representing a file or
        folder entity.  Requires the ID of the provider base folder.  Requires base folder metadata
        if base folder is neither the provider root nor the immediate parent of the entity being
        built.

        :param dict response: metadata for the file or folder from OneDrive API
        :param str base_folder_id: ID of the provider root
        :param dict base_folder_metadata: metadata for the provider root from OneDrive API
        :rtype OneDrivePath:
        :return: a new OneDrivePath object representing the entity in `response`
        """
        if (
            base_folder_id not in ('root', response['parentReference']['id']) and
            base_folder_metadata is None
        ):
            raise Exception('Need metadata for base folder to built correct OneDrivePath')

        parent = response['parentReference']

        absolute_root_path = None
        if parent['driveType'] == 'business':
            absolute_root_path = '/drives/{}/root:'.format(parent['driveId'])
        else:
            absolute_root_path = '/drive/root:'

        parent_path = urlparse.unquote(parent['path'].replace(absolute_root_path, ''))
        if (len(parent_path) == 0):
            names = ['', response['name']]
        else:
            names = parent_path.split('/') + [response['name']]

        ids = [response['parentReference']['id'], response['id']]
        if (len(ids) < len(names)):
            for x in repeat(None, len(names) - len(ids)):
                ids.insert(0, x)

        is_folder = response.get('folder', None) is not None

        nbr_parts_to_keep = len(names)
        if base_folder_metadata is not None:  # need to sanitize base_folder and below
            # calculate depth of base folder below drive root
            # in drive root: 0
            # in subfolder of drive root: 1
            # IS drive root: shouldn't happen
            # etc.
            base_folder_depth = base_folder_metadata['parentReference']['path'].replace(
                absolute_root_path, ''
            ).count('/') + 1
            nbr_parts_to_keep = len(names) - base_folder_depth
        elif base_folder_id != 'root':  # immediate parent is base folder. sanitize
            nbr_parts_to_keep = 2
        else:  # base folder is root, no need to sanitize
            pass

        if nbr_parts_to_keep < len(names):
            keep_idx = nbr_parts_to_keep * -1
            names = names[keep_idx:]
            ids = ids[keep_idx:]

        names[0] = ''
        ids[0] = base_folder_id  # redundant for middle case, but so what.

        return cls('/'.join(names), _ids=ids, folder=is_folder)
