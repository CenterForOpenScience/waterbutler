import os
import hashlib
import zipfile
from datetime import datetime
from waterbutler.core import metadata

ITEM_PREFIX = 'weko:'


def split_path(path):
    assert not path.startswith('/')
    if len(path) == 0:
        return (None, '')
    components = path.split('/')
    drafti = [i for i, c in enumerate(components)
                if len(c) > 0 and not c.startswith(ITEM_PREFIX)]
    if len(drafti) == 0:
        return (path, '')
    indices = components[:drafti[0]]
    if len(indices) == 0:
        return (None, path)
    index_path = '{}/'.format('/'.join(indices))
    assert path.startswith(index_path)
    return (index_path, path[len(index_path):])


def get_files(directory, relative=''):
    files = []
    for f in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, f)):
            files.append(os.path.join(relative, f) if len(relative) > 0 else f)
        elif os.path.isdir(os.path.join(directory, f)):
            for child in get_files(os.path.join(directory, f),
                                   os.path.join(relative, f)
                                   if len(relative) > 0 else f):
                files.append(child)
    return files


class BaseWEKOMetadata(metadata.BaseMetadata):
    @property
    def provider(self):
        return 'weko'

    @property
    def created_utc(self):
        return None


class WEKOItemMetadata(BaseWEKOMetadata, metadata.BaseFileMetadata):
    index = None
    all_indices = None

    def __init__(self, raw, index, all_indices):
        super().__init__(raw)
        self.index = index
        self.all_indices = all_indices

    @property
    def file_id(self):
        return str(self.raw.file_id)

    @property
    def name(self):
        return self.raw.title

    @property
    def content_type(self):
        return None

    @property
    def materialized_name(self):
        return ITEM_PREFIX + self.raw.file_id

    @property
    def path(self):
        target = self.index
        path = ITEM_PREFIX + target.identifier + '/'
        while target.parentIdentifier is not None:
            target = [i for i in self.all_indices
                        if i.identifier == target.parentIdentifier][0]
            path = ITEM_PREFIX + target.identifier + '/' + path
        return '/' + path + ITEM_PREFIX + self.raw.file_id

    @property
    def size(self):
        return None

    @property
    def modified(self):
        return None

    @property
    def etag(self):
        return self.raw.file_id

    @property
    def extra(self):
        return {
            'fileId': self.raw.file_id,
        }


class WEKOIndexMetadata(BaseWEKOMetadata, metadata.BaseFolderMetadata):
    all_indices = None

    def __init__(self, raw, all_indices):
        super().__init__(raw)
        self.all_indices = all_indices

    @property
    def name(self):
        return self.raw.title

    @property
    def materialized_name(self):
        return ITEM_PREFIX + self.raw.identifier

    @property
    def path(self):
        target = self.raw
        path = ITEM_PREFIX + target.identifier + '/'
        while target.parentIdentifier is not None:
            target = [i for i in self.all_indices
                        if i.identifier == target.parentIdentifier][0]
            path = ITEM_PREFIX + target.identifier + '/' + path
        return '/' + path

    @property
    def extra(self):
        return {
            'indexId': self.raw.identifier,
        }


class WEKODraftFileMetadata(BaseWEKOMetadata, metadata.BaseFileMetadata):
    parent_index = None

    def __init__(self, raw, parent_index):
        super().__init__(raw)
        assert parent_index is None or parent_index == '' or parent_index.endswith('/')
        self.modified_time = datetime.utcfromtimestamp(os.path.getmtime(self.raw['filepath']))
        self.parent_index = parent_index
        self.has_import_xml = False
        path, fn = os.path.split(self.raw['filepath'])
        if '/' not in self.raw['path'] and \
           os.path.splitext(fn)[1].lower() == '.zip':
            try:
                with zipfile.ZipFile(self.raw['filepath'], 'r') as zf:
                    self.has_import_xml = 'import.xml' in zf.namelist()
            except:
                pass

    @property
    def path(self):
        if self.parent_index is None:
            return '/' + self.raw['path']
        else:
            return '/' + self.parent_index + self.raw['path']

    @property
    def name(self):
        return self.raw['path'].split('/')[-1]

    @property
    def materialized_name(self):
        return self.name

    @property
    def content_type(self):
        return None

    @property
    def size(self):
        return int(self.raw['bytes'])

    @property
    def modified(self):
        return self.modified_time.strftime('%Y-%m-%d %H:%M:%S')

    @property
    def etag(self):
        if self.parent_index is None:
            code = self.raw['filepath']
        else:
            code = self.parent_index + '\t' + self.raw['filepath']
        return hashlib.sha256(code.encode('utf-8')).hexdigest()

    @property
    def extra(self):
        path, fn = os.path.split(self.raw['filepath'])
        if ITEM_PREFIX in self.raw['path']:
            _, draft_path = split_path(self.raw['path'])
        else:
            draft_path = self.raw['path']
        return {'archivable': '/' not in draft_path,
                'has_import_xml': self.has_import_xml,
                'content_files': [fn]}


class WEKODraftFolderMetadata(BaseWEKOMetadata, metadata.BaseFolderMetadata):
    parent_index = None

    def __init__(self, raw, parent_index):
        super().__init__(raw)
        assert parent_index is None or parent_index == '' or parent_index.endswith('/')
        assert self.raw['path'].endswith('/')
        self.parent_index = parent_index
        self.content_files = get_files(self.raw['filepath'])

    @property
    def name(self):
        return self.raw['path'].split('/')[-2]

    @property
    def materialized_name(self):
        return self.name

    @property
    def path(self):
        if self.parent_index is None:
            return '/' + self.raw['path']
        else:
            return '/' + self.parent_index + self.raw['path']

    @property
    def extra(self):
        if ITEM_PREFIX in self.raw['path']:
            _, draft_path = split_path(self.raw['path'])
        else:
            draft_path = self.raw['path']
        return {'archivable': '/' not in draft_path[:-1],
                'has_import_xml': False,
                'content_files': self.content_files}
