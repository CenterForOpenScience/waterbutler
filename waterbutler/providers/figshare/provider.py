import http
import json
import time
import asyncio
from urllib.parse import urlparse, urlunparse

import aiohttp

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.figshare import metadata
from waterbutler.providers.figshare import settings


class FigsharePath(WaterButlerPath):

    def __init__(self, path, folder: bool, is_public=False, _ids=(),
                 prepend=None):
        super().__init__(path, _ids=_ids, prepend=prepend, folder=folder)
        self.is_public = is_public

    @property
    def identifier_path(self):
        """ Returns a path based on article/file identifiers, relative to the
        provider storage root.  Does NOT include a leading slash.  Calling
        `.identifier_path()` on the storage root returns the empty string.
        """
        if len(self.parts) == 1:
            return ''
        return '/'.join([x.identifier for x in self.parts[1:]]) + ('/' if self.is_dir else '')


class FigshareProvider(provider.BaseProvider):
    """Provider for Figshare repositories.

    **On paths:**  Figshare does not have any notion of paths and has a very
    flat structure. Top level objects are one of the following.

    A 'project' that contains 'articles'. The project can be either public or private.
    A 'collection' that points to private and/or public 'articles' and can itself be either public or private.
    An 'article' that contains 0 or more 'files' and may or may not be associated with a project. Articles may be either public or private.


    'Articles' may contain 'files'.

    'Articles' are one of (currently)ten types. All but one of these 'defined_types' may contain no more then one file. The exception is the 'fileset' 'defined_type' which may contain more then one 'file'

    The FigshareProvider allows for the possibility of either a private 'project' or a private 'collection' as the root of a waterbutler provider instance. This provider's deafult configuration treats 'articles' with a 'defined_type' of 'fileset' as a folder, and all other 'defined_type's as a file.

    In practice, this means that when returning the metadata for the root(/) folder, only 'articles' of 'defined_type' 'fileset' will be returned as a folder. All other 'articles' will be returned as a file if they contain a file and ignored if they do not contain a file.

    Valid FigsharePaths:
        /
        /<article_id for type fileset>/
        /<article_id of any type>/<file_id>

    Invalid FigsharePaths(include but are not limited to):
        /<article_id of any type>
        /<article_id of any type other then fileset>/
        /<article_id of any type>/<file_id>/
        path of any depth greater then 2

    API docs: https://docs.figshare.com/
    """

    NAME = 'figshare'
    BASE_URL = settings.BASE_URL
    VIEW_URL = settings.VIEW_URL
    DOWNLOAD_URL = settings.DOWNLOAD_URL

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.token = self.credentials['token']
        self.container_type = self.settings['container_type']
        self.container_id = self.settings['container_id']

    @property
    def root_path_parts(self):
        return (self.container_type + 's', self.container_id)

    @property
    def default_headers(self):
        return {
            'Authorization': 'token {}'.format(self.token),
        }

    async def make_request(self, method, url, is_public: bool, *args, **kwargs):
        """Add 'account' for private request and super make_request

        :param method: str: HTTP method
        :param url: str: URL
        :param is_public: bool: True if addressing public resource
        :param *args:
        :param **kwargs:

        Subclassed to include handling of is_public argument. 'collection'
        'container_type's may contain public 'article's which are accessed
        through an URN with a different prefix.
        """
        if not is_public:
            url = urlparse(url)
            url_path = url[2].replace('/v2/', '/v2/account/', 1)
            url = urlunparse((url[0], url[1], url_path, url[3], url[4], url[5]))
        if isinstance(kwargs.get('data'), dict):
            kwargs['data'] = json.dumps(kwargs['data'])
        return (await super().make_request(method, url, *args, **kwargs))

    def can_duplicate_names(self):
        """
        Articles may have duplicate titles and files may have duplicate names

        However, this provider does not allow the creation of duplicate files
        and folders.
        """
        return True

    async def validate_path(self, path, **kwargs):
        """
        Validate existance of a path

        ...to the extent possible with v0 API input, and return a FigsharePath
        object.

        :param path: str: identifier_path URN as passed through the v0 API
        :rtype FigsharePath:

        Quirks:
            v0 may pass an identifier_path who's last part is a name and not an
            identifier, in the case of file/folder creation calls.
            validate_path validates parent  and returns a FigsharePath as
            accurately as possible.
        """
        if path == '/':
            return FigsharePath('/',
                                _ids=('', ),
                                folder=True,
                                is_public=False)
        if len(self._path_split(path)) == 2:
            junk, article_id = self._path_split(path)
            file_id = None
        elif len(self._path_split(path)) == 3:
            junk, article_id, file_id = self._path_split(path)
        else:
            raise exceptions.InvalidPathError('Figshare paths may have a depth of no more than two.\n {} is not a valid Figshare path.'.format(path))

        root_article_response = await self.make_request(
            'GET',
            self.build_url(*self.root_path_parts, 'articles'),
            False,
            expects=(200, ),
        )
        # TODO: need better way to get public/private
        # also this call's return is currently busted at figshare
        # https://support.figshare.com/support/tickets/26558
        is_public = False
        for item in await root_article_response.json():
            if '/articles/' + article_id in item['url']:
                article_name = item['title']
                if settings.PRIVATE_IDENTIFIER not in item['url']:
                    is_public = True

        article_segments = (*self.root_path_parts,
                            'articles',
                            article_id)
        if file_id:
            file_response = await self.make_request(
                'GET',
                self.build_url(*article_segments, 'files', file_id),
                is_public,
                expects=(200, 404, ),
            )
            if file_response.status == 200:
                file_response_json = await file_response.json()
                file_name = file_response_json['name']
                return FigsharePath('/' + article_name + '/' + file_name,
                                    _ids=('', article_id, file_id),
                                    folder=False,
                                    is_public=is_public)
            else:
                file_response.close()

        article_response = await self.make_request(
            'GET',
            self.build_url(*article_segments),
            is_public,
            expects=(200, 404, ),
        )
        if article_response.status == 200:
            article_json = await article_response.json()
            if article_json['defined_type'] in settings.FOLDER_TYPES:
                # Case of v0 file creation
                if file_id:
                    ids = ('', article_id, '')
                    folder = False
                    path_urn = '/' + article_name + '/' + file_id
                else:
                    ids = ('', article_id)
                    folder = True
                    path_urn = '/' + article_name + '/'
                return FigsharePath(path_urn, _ids=ids, folder=folder,
                                    is_public=is_public)
        else:
            article_response.close()
        if file_id:
            # Catch for if neither file nor article exist
            raise exceptions.NotFoundError()
        # Return for v0 folder creation
        return FigsharePath(path, _ids=('', ''), folder=True, is_public=False)

    async def validate_v1_path(self, path, **kwargs):
        """
        Validate existance of a path and return a FigsharePath object.

        :param path: str: identifier_path URN as passed through the v1 API
        :rtype FigsharePath:
        """
        if path == '/':
            return FigsharePath('/', _ids=('', ), folder=True, is_public=False)
        if len(self._path_split(path)) == 2:
            junk, article_id = self._path_split(path)
            file_id = None
        elif len(self._path_split(path)) == 3:
            junk, article_id, file_id = self._path_split(path)
        else:
            raise exceptions.InvalidPathError('Figshare paths may have a depth of no more than two.\n {} is not a valid Figshare path.'.format(path))

        root_article_response = await self.make_request(
            'GET',
            self.build_url(*self.root_path_parts, 'articles'),
            False,
            expects=(200, ),
        )
        # TODO: need better way to get public/private
        # also this call's return is currently busted at figshare
        # https://support.figshare.com/support/tickets/26558
        is_public = False
        for item in await root_article_response.json():
            if '/articles/' + article_id in item['url']:
                article_name = item['title']
                if settings.PRIVATE_IDENTIFIER not in item['url']:
                    is_public = True

        article_segments = (*self.root_path_parts,
                            'articles',
                            article_id)
        if file_id:
            file_response = await self.make_request(
                'GET',
                self.build_url(*article_segments, 'files', file_id),
                is_public,
                expects=(200, ),
            )
            file_json = await file_response.json()
            file_name = file_json['name']
            file_response.close()
            if path[-1] == '/':
                raise exceptions.NotFoundError('File paths must not end with "/".  {} not found.'.format(path))
            return FigsharePath('/' + article_name + '/' + file_name,
                                _ids=('', article_id, file_id),
                                folder=False,
                                is_public=is_public)

        article_response = await self.make_request(
            'GET',
            self.build_url(*article_segments),
            is_public,
            expects=(200, ),
        )
        article_json = await article_response.json()
        if article_json['defined_type'] in settings.FOLDER_TYPES:
            if not path[-1] == '/':
                raise exceptions.NotFoundError('Folder paths must end with "/".  {} not found.'.format(path))
            return FigsharePath('/' + article_name + '/', _ids=('', article_id),
                                folder=True, is_public=is_public)
        else:
            raise exceptions.NotFoundError('This article is not configured as a folder defined_type. {} not found.'.format(path))

    async def revalidate_path(self, parent_path, child_name, folder: bool):
        """Attempt to get child's id and return FigsharePath of child

        revalidate_path is used to check for the existance of a child_name/folder
        within the parent. Returning a FigsharePath of child. Child will have _id
        if conflicting child_name/folder exists otherwise _id will be ''.

        :param parent_path: FigsharePath: Path of parent
        :param child_name: str: Name of child
        :param folder: bool: True if child is folder

        Code notes:
        Due to the fact that figshare allows duplicate titles/names for
        articles/files, revalidate_path can not be relied on to always return
        the correct id of an existing child_name. will return the first id that
        matches the folder and child_name arguments or '' if no match.
        """
        urn_parts = (*self.root_path_parts, 'articles')
        if not parent_path.is_root:
            if not folder:
                urn_parts = (*urn_parts, (parent_path.identifier))
            else:
                raise exceptions.NotFoundError('{} is not a valid parent path of folder={}. Folder can only exist at the root level.'.format(parent_path.identifier_path, str(folder)))
        list_children_response = await self.make_request(
            'GET',
            self.build_url(*urn_parts),
            False,
            expects=(200, ),
        )
        child_id = ''
        if not parent_path.is_root:
            article_json = await list_children_response.json()
            if article_json['defined_type'] in settings.FOLDER_TYPES:
                for file in article_json['files']:
                    if file['name'] == child_name:
                        child_id = str(file['id'])
                        break
            else:
                raise exceptions.NotFoundError('{} is not a valid parent path of folder={}. defined_type is {}.'.format(parent_path.identifier_path, str(folder), article_json['defined_type']))
        else:
            root_json = await list_children_response.json()
            articles = await asyncio.gather(*[
                self._get_url_super(article_json['url'])
                for article_json in root_json
            ])
            if folder:
                for article in articles:
                    if article['defined_type'] in settings.FOLDER_TYPES:
                        if article['title'] == child_name:
                            child_id = str(article['id'])  # string?
                            break
            else:
                for article in articles:
                    if not article['defined_type'] in settings.FOLDER_TYPES:
                        for file in article['files']:
                            if file['name'] == child_name:
                                child_id = str(file['id'])  # string?
                                break
        return parent_path.child(child_name, _id=child_id, folder=folder)

    async def _get_url_super(self, url):
        # Use super to avoid is_public logic
        # Allows for taking advantage of asyncio.gather
        response = await super().make_request(
            'GET',
            url,
            expects=(200, ),
        )
        return await response.json()

    async def metadata(self, path, **kwargs):
        """
        Return metadata object for given FigsharePath object

        :param path: FigsharePath: who's Metadata object(s) will be returned
        :rtype FigshareFileMetadata obj or list of Metadata objs:
        """
        if path.is_root:
            articles_response = await self.make_request(
                'GET',
                self.build_url(*self.root_path_parts, 'articles'),
                False,
                expects=(200, ),
            )
            articles_json = await articles_response.json()
            path.is_public = False  # this needed?
            contents = await asyncio.gather(*[
                # TODO: collections may need to use each['url'] for correct URN
                # Use _get_url_super ? figshare API needs to get fixed first.
                self._get_articles_metadata(str(each['id']), path.is_public)
                for each in articles_json
            ])
            return [each for each in contents if each]
        if not path.parts[-1].identifier:
            raise exceptions.NotFoundError(str(path))
        if len(path.parts) > 3:
            raise exceptions.NotFoundError(str(path))
        article_response = await self.make_request(
            'GET',
            self.build_url(*self.root_path_parts,
                           'articles', path.parts[1].identifier),
            path.is_public,
            expects=(200, 404),
        )
        if article_response.status == 404:
            raise exceptions.NotFoundError(str(path))
        article_json = await article_response.json()

        if len(path.parts) == 2:
            if article_json['defined_type'] in settings.FOLDER_TYPES:
                contents = []
                for file in article_json['files']:
                    contents.append(metadata.FigshareFileMetadata(
                                    article_json, raw_file=file))
                return contents
            else:
                raise exceptions.NotFoundError(str(path))

        elif len(path.parts) == 3:
            ret = False
            for file in article_json['files']:
                if file['id'] == int(path.parts[2].identifier):
                    ret = (metadata.FigshareFileMetadata(article_json,
                                                         raw_file=file))
            if ret:
                return ret
            else:
                raise exceptions.NotFoundError()
        else:
            raise exceptions.NotFoundError('{} is not a valid path.'.format(path))

    def _path_split(self, path):
        """Split path dropping trailing '/'

        Convenience method

        :param path: str: Path t be split
        """
        return path.rstrip('/').split('/')

    async def _get_articles_metadata(self, article_id, is_public: bool):
        """Return Figshare*Metadata object for given article_id

        Seperate def to allow for taking advantage of asyncio.gather

        :param article_id: str: article id who's metadata is requested
        :param is_public: bool: True if article to accessed through public URN
        """
        response = await self.make_request(
            'GET',
            self.build_url(*self.root_path_parts, 'articles', article_id),
            is_public,
            expects=(200, ),
        )
        article_json = await response.json()
        if article_json['defined_type'] in settings.FOLDER_TYPES:
            return metadata.FigshareFolderMetadata(article_json)
        elif article_json['files']:
            return metadata.FigshareFileMetadata(article_json)

    async def download(self, path, **kwargs):
        """
        Download a file.

        :param obj path: FigsharePath to file you want to download
        :rtype ResponseWrapper:
        """
        if not path.is_file:
            raise exceptions.NotFoundError(str(path))

        file_metadata = await self.metadata(path)
        download_url = file_metadata.extra['downloadUrl']
        if download_url is None:
            raise exceptions.DownloadError(
                'Download not available',
                code=http.client.FORBIDDEN,
            )
        if path.is_public:
            params = {}
        else:
            params = {'token': self.token}
        headers = {'Host': 'ndownloader.figshare.com', 'Accept': '*/*'}
        resp = await aiohttp.request('GET',
                                     download_url,
                                     params=params,
                                     headers=headers)
        return streams.ResponseStreamReader(resp)

    def path_from_metadata(self, parent_path, metadata):
        """Add child to FigsharePath given child's metadata

        :param parent_path: FigsharePath: of parent
        :param metadata: Figshare*Metadata object of child
        """
        folder = (metadata.kind == 'folder')
        _id = str(metadata.id)
        return parent_path.child(metadata.name, _id=_id, folder=folder)

    async def upload(self, stream, path, **kwargs):
        """ Upload a file to provider.

        Upload a file to provider root or to an article who's defined_type is
        configured to represent a folder.

        :param stream: asyncio.StreamReader: stream to upload
        :param path: FigsharePath: inclusive of file to be uploaded
        :param **kwargs: Will be passed to returned metadata object
        """
        # Create article or retrieve article_id from existing article
        if not path.parent.is_root:
            article_id = path.parent.identifier
        else:
            article_name = json.dumps({'title': path.name})
            if self.container_type == 'project':
                article_resp = await self.make_request(
                    'POST',
                    self.build_url('account',
                                   *self.root_path_parts,
                                   'articles'),
                    'false',
                    data=article_name,
                    expects=(201, ),
                )
                article_json = await article_resp.json()
                article_id = article_json['location'].rsplit('/', 1)[1]
            elif self.container_type == 'collection':
                article_resp = await self.make_request(
                    'POST',
                    self.build_url('account', 'articles'),
                    'false',
                    data=article_name,
                    expects=(201, ),
                )
                articles_json = await article_resp.json()
                article_id = articles_json['location'].rsplit('/', 1)[1]
                article_list = json.dumps({'articles': [article_id]})
                await self.make_request(
                    'POST',
                    self.build_url('account',
                                   *self.root_path_parts,
                                   'articles'),
                    'false',
                    data=article_list,
                    expects=(201, ),
                )
            else:
                raise exceptions.ProviderError('Invalid container_type: {}'.format(self.container_type))
        # Create file. Get file ID
        file_data = json.dumps({'name': path.name, 'size': stream.size})
        file_resp = await self.make_request(
            'POST',
            self.build_url('account', 'articles', article_id, 'files'),
            'false',
            data=file_data,
            expects=(201, ),
        )
        file_json = await file_resp.json()
        file_id = file_json['location'].rsplit('/', 1)[1]
        # Get upload url and file parts info
        # added sleep() as file was not availble right away after getting 201 back.
        # polling with HEADs is another possible solution
        time.sleep(settings.FILE_CREATE_WAIT)
        file_resp = await self.make_request(
            'GET',
            self.build_url('account', 'articles', article_id, 'files', file_id),
            'false',
            expects=(200, 404),
        )
        if file_resp.status == 404:
            raise exceptions.ProviderError('Could not get upload_url. File creation may have taken more than {} seconds to finish.'.format(str(settings.FILE_CREATE_WAIT)))
        file_json = await file_resp.json()
        upload_url = file_json['upload_url']
        token_resp = await self.make_request(
            'GET',
            upload_url,
            'false',
            expects=(200, ),
        )
        token_json = await token_resp.json()
        parts = token_json['parts']  # dict
        # Upload parts
        for part in parts:
            size = part['endOffset'] - part['startOffset'] + 1
            part_number = part['partNo']
            upload_response = await self.make_request(
                'PUT',
                upload_url + '/' + str(part_number),
                'false',
                data=stream._read(size),
                expects=(200, ),
            )
            upload_response.close()

        # Mark upload complete
        upload_response = await self.make_request(
            'POST',
            self.build_url('account', 'articles', article_id, 'files', file_id),
            'false',
            expects=(202, ),
        )
        upload_response.close()
        path = FigsharePath('/' + article_id + '/' + file_id,
                            _ids=(self.container_id, article_id, file_id),
                            folder=False,
                            is_public=False)
        return (await self.metadata(path, **kwargs)), True

    async def delete(self, path, confirm_delete=0, **kwargs):
        """Given a WaterButlerPath, delete that path
        :param path: FigsharePath: Path to be deleted
        :param confirm_delete: int: Must be 1 to confirm root folder delete
        :rtype: None
        :raises: :class:`waterbutler.core.exceptions.NotFoundError`
        :raises: :class:`waterbutler.core.exceptions.DeleteError`

        Quirks:
            If the WaterButlerPath given is for the provider root path, then
            the contents of provider root path will be deleted. But not the
            provider root itself.
        """
        if path.is_root:
            if confirm_delete == 1:
                await self._delete_container_contents(path)
                return
            raise exceptions.DeleteError(
                'confirm_delete=1 is required for deleting root provider folder',
                code=400
            )
        if len(path.parts) == 2:
            if not path.is_folder:
                raise exceptions.NotFoundError(str(path))
            delete_path = (*self.root_path_parts, 'articles', path.parts[1]._id)
        elif len(path.parts) == 3:
            if path.is_folder:
                raise exceptions.NotFoundError(str(path))
            article_response = await self.make_request(
                'GET',
                self.build_url(*self.root_path_parts, 'articles',
                               path.parts[1]._id),
                False,
                expects=(200, ),
            )
            article_json = await article_response.json()
            if article_json['defined_type'] in settings.FOLDER_TYPES:
                delete_path = ('articles', path.parts[1]._id,
                               'files', path.parts[2]._id)
            else:
                delete_path = (*self.root_path_parts, 'articles',
                               path.parts[1]._id)
        delete_article_response = await self.make_request(
            'DELETE',
            self.build_url(*(delete_path)),
            False,
            expects=(204, ),
        )
        delete_article_response.close()

    async def _delete_container_contents(self, path):
        """Delete contents but leave root of Project/Collection

        :param path: FigsharePath to be emptied
        """
        # TODO: Needs logic for skipping public articles in collections
        articles_response = await self.make_request(
            'GET',
            self.build_url(*self.root_path_parts, 'articles'),
            False,
            expects=(200, ),
        )
        articles_json = await articles_response.json()
        for article in articles_json:
            delete_article_response = await self.make_request(
                'DELETE',
                self.build_url(*self.root_path_parts, 'articles',
                               str(article['id'])),
                False,
                expects=(204, ),
            )
            delete_article_response.close()

    async def create_folder(self, path, **kwargs):
        """Create a folder

        Create a folder at `path`. Returns a `FigshareFolderMetadata` object
        if successful.

        :param obj path: FigsharePath obj to create. must be a directory.
        :rtype: :class:`waterbutler.core.metadata.FigshareFolderMetadata`
        :raises: :class:`waterbutler.core.exceptions.CreateFolderError`
        """
        if (len(path.parts) == 2) and path.is_folder:
            article_name = path.parts[-1].value
        else:
            raise exceptions.CreateFolderError(str(path) + ' is not a valid folder creation path. Must be directly off of root and of kind "folder".', code=400)
        article_data = json.dumps({'title': article_name,
                                   'defined_type': 'fileset'})
        create_article_response = await self.make_request(
            'POST',
            self.build_url(*self.root_path_parts, 'articles'),
            False,
            data=article_data,
            expects=(201, ),
            throws=exceptions.CreateFolderError,
        )
        new_article_id = create_article_response.headers['LOCATION'].rstrip('/').split('/')[-1]
        create_article_response.close()
        get_article_response = await self.make_request(
            'GET',
            self.build_url(*self.root_path_parts, 'articles', new_article_id),
            False,
            expects=(200, ),
            throws=exceptions.NotFoundError,
        )
        article_json = await get_article_response.json()

        return metadata.FigshareFolderMetadata(article_json)
