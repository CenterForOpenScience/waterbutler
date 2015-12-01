import json
import http
import asyncio

import logging

from urllib.parse import urlparse

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.onedrive import settings
from waterbutler.providers.onedrive.metadata import OneDriveRevision
from waterbutler.providers.onedrive.metadata import OneDriveFileMetadata
from waterbutler.providers.onedrive.metadata import OneDriveFolderMetadata

logger = logging.getLogger(__name__)

class OneDriveProvider(provider.BaseProvider):
    NAME = 'onedrive'
    BASE_URL = settings.BASE_URL

    def __init__(self, auth, credentials, settings):        
        super().__init__(auth, credentials, settings)        
        self.token = self.credentials['token']
        self.folder = self.settings['folder']
        logger.debug("__init__ credentials:{} settings:{}".format(repr(credentials), repr(settings)))    

    @asyncio.coroutine
    def validate_v1_path(self, path, **kwargs):
        if path == '/':
            return WaterButlerPath(path, prepend=self.folder)

        logger.info('validate_v1_path self::{} path::{}'.format(repr(self), path))
#         implicit_folder = path.endswith('/')
        resp = yield from self.make_request(
            'GET', self.build_url(self.folder),
            expects=(200,),
            throws=exceptions.MetadataError
        )

        data = yield from resp.json()
        logger.info('validate_v1_path data::{}'.format(repr(data)))
        logger.info('validate_v1_path::path{}'.format(path))
        
#         explicit_folder = 'folder' in data.keys()
#         if explicit_folder != implicit_folder:
#             raise exceptions.NotFoundError(str(path))

        return WaterButlerPath(path)#, prepend=self.folder)

    @asyncio.coroutine
    def validate_path(self, path, **kwargs):
        return WaterButlerPath(path, prepend=self.folder)

    @property
    def default_headers(self):
        return {
            'Authorization': 'Bearer {}'.format(self.token),
        }

    @asyncio.coroutine
    def intra_copy(self, dest_provider, src_path, dest_path):
        #  https://dev.onedrive.com/items/copy.htm
        try:
            if self == dest_provider:
                resp = yield from self.make_request(
                    'POST',
                    self.build_url('fileops', 'copy'),
                    data={
                        'root': 'auto',
                        'from_path': src_path.full_path,
                        'to_path': dest_path.full_path,
                    },
                    expects=(200, 201),
                    throws=exceptions.IntraCopyError,
                )
            else:
                from_ref_resp = yield from self.make_request(
                    'GET',
                    self.build_url('copy_ref', 'auto', src_path.full_path),
                )
                from_ref_data = yield from from_ref_resp.json()
                resp = yield from self.make_request(
                    'POST',
                    self.build_url('fileops', 'copy'),
                    data={
                        'root': 'auto',
                        'from_copy_ref': from_ref_data['copy_ref'],
                        'to_path': dest_path,
                    },
                    headers=dest_provider.default_headers,
                    expects=(200, 201),
                    throws=exceptions.IntraCopyError,
                )
        except exceptions.IntraCopyError as e:
            if e.code != 403:
                raise

            yield from dest_provider.delete(dest_path)
            resp, _ = yield from self.intra_copy(dest_provider, src_path, dest_path)
            return resp, False

        # TODO Refactor into a function
        data = yield from resp.json()

        if not data['is_dir']:
            return OneDriveFileMetadata(data, self.folder), True

        folder = OneDriveFolderMetadata(data, self.folder)

        folder.children = []
        for item in data['contents']:
            if item['is_dir']:
                folder.children.append(OneDriveFolderMetadata(item, self.folder))
            else:
                folder.children.append(OneDriveFileMetadata(item, self.folder))

        return folder, True

    @asyncio.coroutine
    def intra_move(self, dest_provider, src_path, dest_path):
        #  https://dev.onedrive.com/items/move.htm
        logger.info('intra_move dest_provider::{} src_path::{} dest_path::{}  self::{}'.format(repr(dest_provider), repr(src_path), repr(dest_path), repr(self)))
        if dest_path.full_path.lower() == src_path.full_path.lower():
            # OneDrive does not support changing the casing in a file name
            raise exceptions.InvalidPathError('In OneDrive to change case, add or subtract other characters.')

        try:
            resp = yield from self.make_request(
                'POST',
                self.build_url('fileops', 'move'),
                data={
                    'root': 'auto',
                    'to_path': dest_path.full_path,
                    'from_path': src_path.full_path,
                },
                expects=(200, ),
                throws=exceptions.IntraMoveError,
            )
        except exceptions.IntraMoveError as e:
            if e.code != 403:
                raise

            yield from dest_provider.delete(dest_path)
            resp, _ = yield from self.intra_move(dest_provider, src_path, dest_path)
            return resp, False

        data = yield from resp.json()

        if not data['is_dir']:
            return OneDriveFileMetadata(data, self.folder), True

        folder = OneDriveFolderMetadata(data, self.folder)

        folder.children = []
        for item in data['contents']:
            if item['is_dir']:
                folder.children.append(OneDriveFolderMetadata(item, self.folder))
            else:
                folder.children.append(OneDriveFileMetadata(item, self.folder))

        return folder, True

    @asyncio.coroutine
    def download(self, path, revision=None, range=None, **kwargs):   
        
        onedriveId = self._get_one_drive_id(path)
        logger.info('oneDriveId:: {} folder:: {} revision::{}'.format(onedriveId, self.folder, revision))        
        downloadUrl = None        
        if revision:
            items = yield from self._revisions_json(path)
            for item in items['value']:
                if item['eTag'] == revision:
                    downloadUrl = item['@content.downloadUrl']                    
                    break                    
        else:
            url = self._build_content_url(onedriveId)                                
            logger.info('url::{}'.format(url))        
            metaData = yield from self.make_request(
                                                    'GET', 
                                                    url, 
                                                    expects=(200, ),
                                                    throws=exceptions.MetadataError
                                                    )
            data = yield from metaData.json()
            logger.info('data::{} downloadUrl::{}'.format(data, downloadUrl))  
            downloadUrl = data['@content.downloadUrl']
        if downloadUrl is None:
            raise exceptions.NotFoundError(str(path))
  
        resp = yield from self.make_request(
            'GET',
            downloadUrl,
            range=range,
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )
        
        return streams.ResponseStreamReader(resp)

    @asyncio.coroutine
    def upload(self, stream, path, conflict='replace', **kwargs):        
        path, exists = yield from self.handle_name_conflict(path, conflict=conflict)
        #  PUT /drive/items/{parent-id}/children/{filename}/content
        #  TODO: uploads to sub-folders: upload url:https://api.onedrive.com/v1.0/drive/items/0/children/75BFE374EBEB1211%21118/owl.jpeg/content path:WaterButlerPath('/75BFE374EBEB1211!118/owl.jpeg', prepend='75BFE374EBEB1211!107') str(path):/75BFE374EBEB1211!118/owl.jpeg kwargs:{'nid': 'qua5g', 'action': 'upload', 'provider': 'onedrive'}
        
        #  path:WaterButlerPath('/75BFE374EBEB1211!118/onedrive-revisions.json', prepend='75BFE374EBEB1211!107') 
        #  str(path):/75BFE374EBEB1211!118/onedrive-revisions.json 
        #  str(full_path):75BFE374EBEB1211!107/75BFE374EBEB1211!118/onedrive-revisions.json
        
        fileName = self._get_one_drive_id(path)
        path = self._get_sub_folder_path(path, fileName) #  urlparse(path.full_path.replace(fileName, '')).path.split('/')[-2]
        upload_url = self.build_url(path ,'children', fileName, "content")
        
        logger.info("upload url:{} path:{} str(path):{} str(full_path):{} self:{}".format(upload_url, repr(path), str(path), str(path), repr(self.folder)))
        
        resp = yield from self.make_request(
            'PUT',
            upload_url,
            headers={'Content-Length': str(stream.size)},
            data=stream,
            expects=(201, ),
            throws=exceptions.UploadError,
        )

        data = yield from resp.json()
        logger.info('upload:: data:{}'.format(data))        
        return OneDriveFileMetadata(data, self.folder), not exists

    @asyncio.coroutine
    def delete(self, path, **kwargs):
        one_drive_id = self._get_one_drive_id(path)        
        logger.info("delete::id::{}".format(one_drive_id))
                         
        yield from self.make_request(
            'DELETE',
            self.build_url(one_drive_id),
            data={},
            expects=(204, ),
            throws=exceptions.DeleteError,
        )

    @asyncio.coroutine
    def metadata(self, path, revision=None, **kwargs):
        logger.debug('metadata path::{} revision::{} kwargs:{}  token:{}'.format(repr(path.full_path), repr(revision), repr(kwargs), self.token))
        if revision:
            url = self.build_url('revisions', 'auto', path.full_path, rev_limit=250)

        else:
            if (path.full_path == '0/'):
                url = self.build_url('root', expand='children') #  handle when OSF is linked to root onedrive
            elif str(path) == '/':
                url = self.build_url(path.full_path, expand='children') #  OSF lined to sub folder
            else:
                url = self.build_url(str(path), expand='children')  #  handles root/sub1, root/sub1/sub2

        logger.debug('metadata url::{} path::{} fullpath::{}'.format(repr(url), repr(path), path.full_path))

        resp = yield from self.make_request(
            'GET', url,
            expects=(200, 400, ),
            throws=exceptions.MetadataError
        )

        data = yield from resp.json()
        logger.debug("metadata data::{}".format(repr(data)))
        
#  TODO: revisions?
#         if revision:
#             try:
#                 data = next(v for v in (yield from resp.json()) if v['rev'] == revision)
#             except StopIteration:
#                 raise exceptions.NotFoundError(str(path))


        if data.get('deleted'): 
            raise exceptions.MetadataError(
                "Could not retrieve {kind} '{path}'".format(
                    kind='folder' if data['folder'] else 'file',
                    path=path,
                ),
                code=http.client.NOT_FOUND,
            )
        

        if 'folder' in data.keys():            
            ret = []
            if 'children' in data.keys():
                for item in data['children']:
                    if 'folder' in item.keys():
                        ret.append(OneDriveFolderMetadata(item, self.folder))
                    else:
                        ret.append(OneDriveFileMetadata(item, self.folder))
            return ret

        return OneDriveFileMetadata(data, self.folder)

    @asyncio.coroutine
    def revisions(self, path, **kwargs):
        #  https://dev.onedrive.com/items/view_delta.htm        
        data = yield from self._revisions_json(path, **kwargs)
        logger.info('revisions: data::{}'.format(data['value']))
 
        return [
            OneDriveRevision(item)
            for item in data['value']
            if not item.get('deleted')
        ]
        
    @asyncio.coroutine
    def _revisions_json(self, path, **kwargs):
        #  https://dev.onedrive.com/items/view_delta.htm
        #  TODO: 2015-11-29 - onedrive only appears to return the last delta for a token, period.  Not sure if there is a work around, from the docs: "The delta feed shows the latest state for each item, not each change. If an item were renamed twice, it would only show up once, with its latest name."
        response = yield from self.make_request(
            'GET',
            self.build_url(str(path), 'view.delta', top=250),
            expects=(200, ),
            throws=exceptions.RevisionsError
        )
        data = yield from response.json()
        logger.info('revisions: data::{}'.format(data['value']))
 
        return data

    @asyncio.coroutine
    def create_folder(self, path, **kwargs):
        """
        :param str path: The path to create a folder at
        """
        #  https://dev.onedrive.com/items/create.htm
        #  PUT /drive/items/{parent-id}:/{name}
        #  In the request body, supply a JSON representation of a Folder Item, as shown below.
        WaterButlerPath.validate_folder(path)

        folderName = path.full_path.split('/')[-2]
        parentFolder = path.full_path.split('/')[-3]
        upload_url = self.build_url(parentFolder, 'children')
        
        logger.info("upload url:{} path:{} parentFolder:{} folderName:{}".format(upload_url, repr(path), str(parentFolder), repr(folderName)))
        payload = {
                    'name': folderName,
                    'folder': {},
                     "@name.conflictBehavior": "rename"
                }

        resp = yield from self.make_request(
            'POST',
            upload_url,
            data=json.dumps(payload),
            headers = {'content-type': 'application/json'},
            expects=(201, ),
            throws=exceptions.CreateFolderError,
        )

        data = yield from resp.json()
        logger.info('upload:: data:{}'.format(data))        
        return OneDriveFolderMetadata(data, self.folder)        

    def can_intra_copy(self, dest_provider, path=None):
        return type(self) == type(dest_provider)

    def can_intra_move(self, dest_provider, path=None):
        return self == dest_provider

    def _build_content_url(self, *segments, **query):
        return provider.build_url(settings.BASE_CONTENT_URL, *segments, **query)
    
    def _get_one_drive_id(self, path): 
        return path.full_path[path.full_path.rindex('/') + 1:]
    
    def _get_sub_folder_path(self, path, fileName):
        return urlparse(path.full_path.replace(fileName, '')).path.split('/')[-2]