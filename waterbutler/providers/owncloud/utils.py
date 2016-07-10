import xml.etree.ElementTree as ET
from urllib import parse

from waterbutler.providers.owncloud.metadata import OwnCloudFileMetadata
from waterbutler.providers.owncloud.metadata import OwnCloudFolderMetadata


def strip_dav_path(path):
    """Removes the leading "remote.php/webdav" path from the given path
    :param path: path containing the remote DAV path "remote.php/webdav"
    :returns: path stripped of the remote DAV path
    """
    if 'remote.php/webdav' in path:
        return path.split('remote.php/webdav')[1]
    return path


async def parse_dav_response(content, folder, skip_first=False):
    items = []
    tree = ET.fromstring(content)

    if skip_first:
        tree = tree[1:]

    for child in tree:
        href = parse.unquote(strip_dav_path(child.find('{DAV:}href').text))

        file_type = 'file'
        if href[-1] == '/':
            file_type = 'dir'

        file_attrs = {}
        attrs = child.find('{DAV:}propstat').find('{DAV:}prop')

        for attr in attrs:
            file_attrs[attr.tag] = attr.text

        if file_type == 'file':
            items.append(OwnCloudFileMetadata(href, folder, file_attrs))
        else:
            items.append(OwnCloudFolderMetadata(href, folder, file_attrs))
    return items
