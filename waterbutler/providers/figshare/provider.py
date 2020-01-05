import json
import asyncio
import hashlib
import logging
from typing import Tuple
from http import HTTPStatus

from waterbutler.core.streams import CutoffStream
from waterbutler.core import exceptions, provider, streams

from waterbutler.providers.figshare.path import FigsharePath
from waterbutler.providers.figshare import settings as pd_settings
from waterbutler.providers.figshare.metadata import (FigshareFileMetadata,
                                                     FigshareFolderMetadata,
                                                     FigshareFileRevisionMetadata, )

logger = logging.getLogger(__name__)


class FigshareProvider:
    """Provider for Figshare repositories.

    **On paths:**  Figshare does not have any notion of paths and has a very flat structure. Top
    level objects are one of the following.

    A 'project' that contains 'articles'. The project can be either public or private.
    A 'collection' that points to private and/or public 'articles' and can itself be either public
    or private.
    An 'article' that contains 0 or more 'files' and may or may not be associated with a project.
    Articles may be either public or private.


    'Articles' may contain 'files'.

    'Articles' are one of (currently) ten types. All but one of these 'defined_types' may contain no
    more than one file. The exception is the 'fileset' 'defined_type' which may contain more than
    one 'file'.

    The FigshareProvider allows for the possibility of a private 'article', a private 'project', or
    a private 'collection' as the root of a waterbutler provider instance. The FigshareProvider's
    default configuration treats 'articles' with a 'defined_type' of 'fileset' as a folder, and all
    other 'defined_type's as a file.

    In practice, this means that when returning the metadata for the root(/) folder, only 'articles'
    of 'defined_type' 'fileset' will be returned as a folder. All other 'articles' will be returned
    as a file if they contain a file and ignored if they do not contain a file.

    If the root is configured as a provider, it will contain 0 or more files.

    Valid FigsharePaths for root project/collection::

        /
        /<article_id for type fileset>/ (default configuration)
        /<article_id of any type>/<file_id>

    Valid FigsharePaths for root article::

        /
        /<file_id>

    Invalid FigsharePaths for root project/collection examples::

        /<article_id of any type>
        /<article_id of any type other then fileset>/ (default configuration)
        /<article_id of any type>/<file_id>/
        path of any depth greater then 2

    Invalid FigsharePaths for root article examples::

        /<any_id>/
        /<any_id other then a file_id>
        path of any depth greater then 1

    API docs: https://docs.figshare.com/
    """

    def __new__(cls, auth, credentials, settings, **kwargs):

        if settings['container_type'] == 'project':
            return FigshareProjectProvider(
                auth, credentials, dict(settings, container_id=settings['container_id']), **kwargs
            )

        if settings['container_type'] in pd_settings.ARTICLE_CONTAINER_TYPES:
            return FigshareArticleProvider(
                auth, credentials, dict(settings, container_id=settings['container_id']), **kwargs
            )

        raise exceptions.ProviderError(
            'Invalid "container_type" {0}'.format(settings['container_type'])
        )


class BaseFigshareProvider(provider.BaseProvider):

    NAME = 'figshare'
    BASE_URL = pd_settings.BASE_URL
    VIEW_URL = pd_settings.VIEW_URL
    DOWNLOAD_URL = pd_settings.DOWNLOAD_URL
    VALID_CONTAINER_TYPES = pd_settings.VALID_CONTAINER_TYPES
    ARTICLE_CONTAINER_TYPES = pd_settings.ARTICLE_CONTAINER_TYPES

    def __init__(self, auth, credentials, settings, **kwargs):
        super().__init__(auth, credentials, settings, **kwargs)
        self.token = self.credentials['token']
        self.container_type = self.settings['container_type']
        if self.container_type not in self.VALID_CONTAINER_TYPES:
            raise exceptions.ProviderError('{} is not a valid container type.'.format(self.container_type))
        # Normalize all article container types to "article"
        if self.container_type in self.ARTICLE_CONTAINER_TYPES:
            self.container_type = 'article'
        self.container_id = self.settings['container_id']
        self.metrics.add('container', {
            'given_type': self.settings['container_type'],
            'actual_type': self.container_type,
        })

    @property
    def root_path_parts(self):
        return (self.container_type + 's', self.container_id)

    @property
    def default_headers(self):
        return {
            'Authorization': 'token {}'.format(self.token),
        }

    def build_url(self, is_public: bool, *segments, **query) -> str:  # type: ignore
        r"""A nice wrapper around furl, builds urls based on self.BASE_URL

        :param bool is_public: ``True`` if addressing public resource
        :param tuple \*segments: A tuple of strings joined into ``/foo/bar/``
        :param dict \*\*query: A dictionary that will be turned into query parameters ``?foo=bar``
        :rtype: str

        Overrides its parent's ``build_url()`` by building a URL that points to the public access
        API of an article or its files if the ``is_public`` argument is set.  This is a special
        design for figshare collections which may contain public articles that don't belong to the
        current figshare-OSF OAuth user.

        Quirk: Due to the facts that 1) the public URL Waterbulter builds no longer works due to
               figshare-side changes and that 2) the collections stuff is still WIP, we decide to
               ignore the ``is_public`` argument for now and always build private API URLs.

        TODO [SVCS-996]: fix the public API and set public only when the container is a collection
        TODO [SVCS-996]: set the public flag only when the container is a collection
        """
        if is_public:
            logger.debug('figshare provider is yet to build the public API URL correctly. '
                         'Switch back to use the private one instead')
        segments = ('account', (*segments))
        return (super().build_url(*segments, **query))

    async def make_request(self, method, url, *args, **kwargs):
        r"""JSONifies ``data`` kwarg, if present and a ``dict``.

        :param str method: HTTP method
        :param str url: URL
        :param tuple \*args:
        :param dict \*\*kwargs:
        """
        if isinstance(kwargs.get('data'), dict):
            kwargs['data'] = json.dumps(kwargs['data'])
        return await super().make_request(method, url, *args, **kwargs)

    def can_duplicate_names(self):
        """Figshare allows articles to have duplicate titles and files to have duplicate names, but
        does not allow the creation of duplicate files and folders.
        """
        return False

    async def _get_url_super(self, url):
        # Use super to avoid is_public logic
        # Allows for taking advantage of asyncio.gather
        response = await super().make_request('GET', url, expects=(200, ))
        return await response.json()

    def _path_split(self, path):
        """Strip trailing slash from path string, then split on remaining slashes.

        :param str path: url path string to be split.
        """
        return path.rstrip('/').split('/')

    async def download(self, path: FigsharePath,  # type: ignore
                       range: Tuple[int, int] = None, **kwargs) -> streams.ResponseStreamReader:
        """Download the file identified by ``path`` from this project.

        :param FigsharePath path: FigsharePath to file you want to download
        :rtype streams.ResponseStreamReader:
        """
        if not path.is_file:
            raise exceptions.NotFoundError(str(path))

        file_metadata = await self.metadata(path)
        # The file download response for files (currently private ones only) does not provide the
        # Content-Length header, of which the value are used to build the response stream and to
        # set the stream size. This is not a problem for file download itself but it breaks move /
        # copy from the figshare provider since the following upload-to-dest step fails on `None`
        # stream size. Thus, save the file size here in advance and provide it later when building
        # the response stream. `ResponseStreamReader` is smart enough to select the Content-Length
        # header if provided.
        file_size = file_metadata.size  # type: ignore
        download_url = file_metadata.extra['downloadUrl']  # type: ignore
        if download_url is None:
            raise exceptions.DownloadError('Download not available', code=HTTPStatus.FORBIDDEN)

        logger.debug('requested-range:: {}'.format(range))
        params = {} if file_metadata.is_public else {'token': self.token}  # type: ignore
        resp = await self.make_request(
            'GET',
            download_url,
            range=range,
            params=params,
            allow_redirects=False,
        )

        if resp.status >= 400:
            await resp.release()
            raise exceptions.DownloadError('Download not available', code=resp.status)

        # When a file has been published, the figshare download request returns a redirection URL
        # which downloads the file directly from its backend storage (currently Amazon S3). The new
        # request does not need any auth at all. More importantly, the default figshare auth header
        # breaks the download since S3 API does not understand figshare API. Thus, the provider
        # must use `no_auth_header=True` to inform `super().make_request()` to drop the header.
        if resp.status in (302, 301):
            await resp.release()
            if range:
                range_header = {'Range': self._build_range_header(range)}
                resp = await super().make_request('GET', resp.headers['location'],
                                                  headers=range_header, no_auth_header=True)
            else:
                resp = await super().make_request('GET', resp.headers['location'],
                                                  no_auth_header=True)

        return streams.ResponseStreamReader(resp, size=file_size)

    def path_from_metadata(self, parent_path, metadata):
        """Build FigsharePath for child entity given child's metadata and parent's path object.

        **HOWEVER** if ``parent_path`` represents a project root and ``metadata`` represents a
        non-dataset article (a file from WB's perspective), then `path_from_metadata` needs to
        skip the intermediate article container and return a path that points directly to the file.

        :param FigsharePath parent_path: path obj for child's parent
        :param metadata: Figshare*Metadata object for child
        """

        if parent_path.is_root and metadata.kind != 'folder':
            intermediate_path = parent_path.child(metadata.article_name,
                                                  _id=str(metadata.extra['articleId']),
                                                  folder=True)
            return intermediate_path.child(metadata.name, _id=str(metadata.extra['fileId']),
                                           folder=(metadata.kind == 'folder'))

        return parent_path.child(metadata.name, _id=str(metadata.id),
                                 folder=(metadata.kind == 'folder'))

    async def revisions(self, path, **kwargs):
        # Public articles have revisions, but projects, collections, and private articles do not.
        # For now, return a single Revision labeled "latest".
        return [FigshareFileRevisionMetadata()]

    async def _upload_file(self, article_id, name, stream):
        """Uploads a file to Figshare and returns the file id.

        :param str article_id: the id of the parent article
        :param str name: the name of the file
        :param stream: the file stream to upload
        :rtype: `str`
        :return: id of new file
        """
        # Process for creating a file:

        # 1. Get file ID
        file_id = await self._make_file_placeholder(article_id, name, stream.size)

        # 2. Get upload url and file parts info
        # added sleep() as file was not availble right away after getting 201 back.
        # polling with HEADs is another possible solution
        await asyncio.sleep(pd_settings.FILE_CREATE_WAIT)
        upload_url, parts = await self._get_file_upload_url(article_id, file_id)

        # 3. Upload parts
        self.metrics.add('upload.parts.count', len(parts))
        await self._upload_file_parts(stream, upload_url, parts)

        # 4. Mark upload complete
        await self._mark_upload_complete(article_id, file_id)

        return file_id

    async def _make_file_placeholder(self, article_id, name, size):
        """Create a placeholder for a file to be uploaded later.  Takes the id of the parent
        article, a name for the file, and the size.  Returns the id set aside for the file.

        :param str article_id: the id of the parent article
        :param str name: the name of the file
        :param int size: the size of the file
        :returns str: the id of the file placeholder
        """
        file_resp = await self.make_request(
            'POST',
            self.build_url(False, 'articles', article_id, 'files'),
            data=json.dumps({'name': name, 'size': size}),
            expects=(201, ),
        )
        file_json = await file_resp.json()
        return file_json['location'].rsplit('/', 1)[1]

    async def _get_file_upload_url(self, article_id, file_id):
        """Request an upload url and partitioning spec from Figshare.
        See: https://docs.figshare.com/api/file_uploader/

        :param str article_id: the id of the parent article
        :param str file_id: the name of the file
        :returns (str, list): the upload url and the parts specification
        """
        # TODO: retry with backoff
        resp = await self.make_request(
            'GET',
            self.build_url(False, 'articles', article_id, 'files', file_id),
            expects=(200, 404),
        )
        if resp.status == 404:
            await resp.release()
            raise exceptions.ProviderError(
                'Could not get upload_url. File creation may have taken more '
                'than {} seconds to finish.'.format(str(pd_settings.FILE_CREATE_WAIT)))

        upload_json = await resp.json()
        upload_url = upload_json['upload_url']

        parts_resp = await self.make_request('GET', upload_url, expects=(200, ),)
        parts_json = await parts_resp.json()
        return upload_url, parts_json['parts']  # str, list

    async def _upload_file_parts(self, stream, upload_url, parts):
        """Takes a stream, the upload url, and a list of parts to upload, and send the chunks
        dictated by ``parts`` to figshare.
        See: https://docs.figshare.com/api/file_uploader/

        :param stream: the file stream to upload
        :param str upload_url: the base url to upload to
        :param list parts: a structure describing the expected partitioning of the file
        """
        for part in parts:
            size = part['endOffset'] - part['startOffset'] + 1
            part_number = part['partNo']
            upload_stream = CutoffStream(stream, cutoff=size)
            logger.debug('File part {}: stream-size:{} want-size:{}'.format(part_number,
                                                                            stream.size, size))
            upload_response = await self.make_request(
                'PUT',
                upload_url + '/' + str(part_number),
                headers={'Content-Length': str(size)},
                data=upload_stream,
                expects=(200, ),
            )
            await upload_response.release()

    async def _mark_upload_complete(self, article_id, file_id):
        """Signal to Figshare that all of the parts of the file have been uploaded successfully.
        See: https://docs.figshare.com/api/file_uploader/

        :param str article_id: the id of the parent article
        :param str file_id: the name of the file
        """
        resp = await self.make_request(
            'POST',
            self.build_url(False, 'articles', article_id, 'files', file_id),
            expects=(202, ),
        )
        await resp.release()


class FigshareProjectProvider(BaseFigshareProvider):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def validate_v1_path(self, path: str, **kwargs) -> FigsharePath:
        """Take a string path from the url and attempt to map it to an entity within this project.
        If the entity is found, returns a FigsharePath object with the entity identifiers included.
        Otherwise throws a 404 Not Found. Will also assert that the entity type inferred from the
        path matches the type of the entity at that url.

        :param str path: entity path from the v1 API
        :rtype FigsharePath:
        """

        if path == '/':
            # Root path should always be private api-wise since project and collection itself must
            # be owned by the fighsare-OSF OAuth user.
            return FigsharePath('/', _ids=('', ), folder=True, is_public=False)

        # Step 0: Preprocess the string path.
        path_parts = self._path_split(path)
        if len(path_parts) not in (2, 3):
            raise exceptions.InvalidPathError('{} is not a valid Figshare path.'.format(path))
        article_id = path_parts[1]
        file_id = path_parts[2] if len(path_parts) == 3 else None

        # Step 1: Get a list of all articles in the project.
        articles = await self._get_all_articles()

        # Step 2: Find the article; set `article_name`, `is_public`; and prepare `article_segments`.
        is_public = False
        article_name = None
        for article in articles:
            if '/articles/' + article_id in article['url']:
                article_name = article['title']
                is_public = article['published_date'] is not None
                break
        # Raise error earlier instead of on 404.  Please note that this is different than V0.
        if not article_name:
            raise exceptions.NotFoundError('Path {} with article ID {} not found in the project\'s '
                                           'article list'.format(path, article_id))
        article_segments = (*self.root_path_parts, 'articles', article_id)

        # Step 3.1: if the path is a file
        if file_id:
            file_response = await self.make_request(
                'GET',
                self.build_url(is_public, *article_segments, 'files', file_id),
                expects=(200, ),
            )
            file_json = await file_response.json()
            file_name = file_json['name']
            if path[-1] == '/':
                raise exceptions.NotFoundError('File paths must not end with "/". '
                                               '{} not found.'.format(path))
            return FigsharePath('/' + article_name + '/' + file_name,
                                _ids=(self.container_id, article_id, file_id),
                                folder=False,
                                is_public=is_public)

        # Step 3.2: if the path is a folder
        article_response = await self.make_request(
            'GET',
            self.build_url(is_public, *article_segments),
            expects=(200, ),
        )
        article_json = await article_response.json()
        if article_json['defined_type'] in pd_settings.FOLDER_TYPES:
            if not path[-1] == '/':
                raise exceptions.NotFoundError('Folder paths must end with "/". '
                                               '{} not found.'.format(path))
            return FigsharePath('/' + article_name + '/', _ids=(self.container_id, article_id),
                                folder=True, is_public=is_public)
        raise exceptions.NotFoundError('This article is not configured as a folder defined_type. '
                                       '{} not found.'.format(path))

    async def validate_path(self, path, **kwargs):
        """Take a string path from the url and attempt to map it to an entity within this project.
        If the entity is found, returns a FigsharePath object with the entity identifiers included.
        Otherwise returns a FigsharePath with empty identifiers.

        :param str path: identifier_path URN as passed through the v0 API
        :rtype FigsharePath:

        Quirks:

        * v0 may pass an identifier_path whose last part is a name and not an identifier, in the
          case of file/folder creation calls.

        * validate_path validates parent and returns a FigsharePath as accurately as possible.
        """
        if path == '/':
            return FigsharePath('/', _ids=('', ), folder=True, is_public=False)

        path_parts = self._path_split(path)
        if len(path_parts) not in (2, 3):
            raise exceptions.InvalidPathError('{} is not a valid Figshare path.'.format(path))
        article_id = path_parts[1]
        file_id = path_parts[2] if len(path_parts) == 3 else None

        articles = await self._get_all_articles()

        is_public = False
        article_name = None
        for article in articles:
            if '/articles/' + article_id in article['url']:
                article_name = article['title']
                is_public = article['published_date'] is not None
                break

        article_segments = (*self.root_path_parts, 'articles', article_id)

        if file_id:
            file_response = await self.make_request(
                'GET',
                self.build_url(is_public, *article_segments, 'files', file_id),
                expects=(200, 404, ),
            )
            if file_response.status == 200:
                file_response_json = await file_response.json()
                file_name = file_response_json['name']
                return FigsharePath('/' + article_name + '/' + file_name,
                                    _ids=(self.container_id, article_id, file_id),
                                    folder=False,
                                    is_public=is_public)
            await file_response.release()

        article_response = await self.make_request(
            'GET',
            self.build_url(is_public, *article_segments),
            expects=(200, 404, ),
        )
        if article_response.status == 200:
            article_json = await article_response.json()
            if article_json['defined_type'] in pd_settings.FOLDER_TYPES:
                # Case of v0 file creation
                if file_id:
                    ids = ('', article_id, '')
                    folder = False
                    path_urn = '/' + article_name + '/' + file_id
                else:
                    ids = ('', article_id)
                    folder = True
                    path_urn = '/' + article_name + '/'
                return FigsharePath(path_urn, _ids=ids, folder=folder, is_public=is_public)
        else:
            await article_response.release()

        if file_id:
            # Catch for if neither file nor article exist
            raise exceptions.NotFoundError(path)

        # Return for v0 folder creation
        return FigsharePath(path, _ids=('', ''), folder=True, is_public=False)

    async def revalidate_path(self, parent_path, child_name, folder):
        """Look for file or folder named ``child_name`` under ``parent_path``. If it finds a match,
        it returns a FigsharePath object with the appropriate ids set.  Otherwise, it returns a
        FigsharePath where the ids are set to ``None``.

        Due to the fact that figshare allows duplicate titles/names for
        articles/files, revalidate_path can not be relied on to always return
        the correct id of an existing child_name. It will return the first id that
        matches the folder and child_name arguments or '' if no match.

        :param FigsharePath parent_path: Path of parent
        :param str child_name: Name of child
        :param bool folder: ``True`` if child is folder
        :rtype: ``FigsharePath``
        :return: a FigsharePath object, with ids set if a match was found
        """
        parent_is_folder = False
        urn_parts = (*self.root_path_parts, 'articles')
        child_id = None
        if not parent_path.is_root:  # parent is fileset or article
            if not folder:  # child is article/file
                list_children_response = await self.make_request(
                    'GET',
                    self.build_url(False, *urn_parts, parent_path.identifier),
                    expects=(200, ),
                )
                article_json = await list_children_response.json()
                for file in article_json['files']:
                    if file['name'] == child_name:
                        child_id = str(file['id'])
                        break
            return parent_path.child(child_name, _id=child_id, folder=folder,
                                     parent_is_folder=parent_is_folder)
        # parent is root
        children = await self._get_all_articles()
        articles = await asyncio.gather(*[
            self._get_url_super(article_json['url'])
            for article_json in children
        ])
        for article in articles:
            is_folder = article['defined_type'] in pd_settings.FOLDER_TYPES
            article_id = str(article['id'])
            article_name = str(article['title'])
            if folder != is_folder:
                continue
            elif folder:
                if article_name == child_name:
                    child_id = article_id
                    break
            else:
                parent_is_folder = False
                for file in article['files']:
                    if file['name'] == child_name:
                        parent_path = parent_path.child(article_name, _id=article_id, folder=False)
                        child_id = str(file['id'])
                        break

        return parent_path.child(child_name, _id=child_id, folder=folder,
                                 parent_is_folder=parent_is_folder)

    async def upload(self, stream, path, conflict='replace', **kwargs):
        r"""Upload a file to provider root or to an article whose defined_type is
        configured to represent a folder.

        :param asyncio.StreamReader stream: stream to upload
        :param FigsharePath path: FigsharePath to upload the file to.
        :param dict \*\*kwargs: Will be passed to returned metadata object
        """
        if path.identifier and conflict == 'replace':
            raise exceptions.UnsupportedOperationError('Files in Figshare cannot be updated')

        path, exists = await self.handle_name_conflict(path, conflict=conflict)
        if not path.parent.is_root:
            parent_resp = await self.make_request(
                'GET',
                self.build_url(False, *self.root_path_parts, 'articles', path.parent.identifier),
                expects=(200, ),
            )
            parent_json = await parent_resp.json()
            if not parent_json['defined_type'] in pd_settings.FOLDER_TYPES:
                del path._parts[1]

        # Create article or retrieve article_id from existing article
        if not path.parent.is_root:
            article_id = path.parent.identifier
        else:
            article_name = json.dumps({'title': path.name})
            if self.container_type == 'project':
                article_id = await self._create_article(article_name)
            elif self.container_type == 'collection':
                # TODO don't think this is correct.  Probably should POST to /accounts/articles
                article_id = await self._create_article(article_name)
                article_list = json.dumps({'articles': [article_id]})
                await self.make_request(
                    'POST',
                    self.build_url(False, *self.root_path_parts, 'articles'),
                    data=article_list,
                    expects=(201, ),
                )

        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))
        file_id = await self._upload_file(article_id, path.name, stream)

        # Build new file path and return metadata
        path = FigsharePath('/' + article_id + '/' + file_id,
                            _ids=(self.container_id, article_id, file_id),
                            folder=False,
                            is_public=False)
        metadata = await self.metadata(path, **kwargs)
        if (
            (not metadata.extra['hashingInProgress']) and
            stream.writers['md5'].hexdigest != metadata.extra['hashes']['md5']
        ):
            raise exceptions.UploadChecksumMismatchError()

        return metadata, True

    async def create_folder(self, path, **kwargs):
        """Create a folder at ``path``. Returns a `FigshareFolderMetadata` object if successful.

        :param FigsharePath path: FigsharePath representing the folder to create
        :rtype: :class:`waterbutler.core.metadata.FigshareFolderMetadata`
        :raises: :class:`waterbutler.core.exceptions.CreateFolderError`
        """
        if (len(path.parts) == 2) and path.is_folder:
            article_name = path.parts[-1].value
        else:
            raise exceptions.CreateFolderError(
                'Only projects and collections may contain folders. Unable to create '
                '"{}/"'.format(path.name),
                code=400,
            )

        # Quirks: use Dataset (3) as the article type during figshare folder creation instead of the
        #         deprecated Fileset (4).  Internally figshare converts Fileset to Dataset anyway.
        article_data = json.dumps({'title': article_name, 'defined_type': 'dataset'})
        article_id = await self._create_article(article_data)
        get_article_response = await self.make_request(
            'GET',
            self.build_url(False, *self.root_path_parts, 'articles', article_id),
            expects=(200, ),
            throws=exceptions.CreateFolderError,
        )
        article_json = await get_article_response.json()

        return FigshareFolderMetadata(article_json)

    async def delete(self, path, confirm_delete=0, **kwargs):
        """Delete the entity at ``path``.

        :param FigsharePath path: Path to be deleted
        :param int confirm_delete: Must be 1 to confirm root folder delete
        :rtype: None
        :raises: :class:`waterbutler.core.exceptions.NotFoundError`
        :raises: :class:`waterbutler.core.exceptions.DeleteError`

        Quirks:

        * If the FigsharePath given is for the provider root path, then the contents of the
          provider root path will be deleted, but not the provider root itself.
        """
        if not path.identifier:
            raise exceptions.NotFoundError(str(path))

        if path.is_root:
            if confirm_delete == 1:
                return await self._delete_container_contents()
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
                self.build_url(False, *self.root_path_parts, 'articles', path.parts[1]._id),
                expects=(200, ),
            )
            article_json = await article_response.json()
            if article_json['defined_type'] in pd_settings.FOLDER_TYPES:
                delete_path = ('articles', path.parts[1]._id, 'files', path.parts[2]._id)
            else:
                delete_path = (*self.root_path_parts, 'articles', path.parts[1]._id)

        delete_article_response = await self.make_request(
            'DELETE',
            self.build_url(False, *delete_path),
            expects=(204, ),
        )
        await delete_article_response.release()

    async def metadata(self, path, **kwargs):
        """Return metadata for entity identified by ``path`` under the parent project.

        :param FigsharePath path: entity whose metadata will be returned
        :rtype: FigshareFileMetadata obj or list of Metadata objs
        """
        if path.is_root:
            # TODO: fix this similar to `validate_path_v1()`
            # NOTE: this won't work for figshare collections since calling `_get_article_metadata()`
            #       on items not owned by the OSF/auth user will fail with 403.
            path.is_public = False
            contents = await asyncio.gather(*[
                # TODO: collections may need to use each['url'] for correct URN
                # Use _get_url_super ? figshare API needs to get fixed first.
                self._get_article_metadata(str(each['id']), path.is_public)
                for each in await self._get_all_articles()
            ])
            return [each for each in contents if each]

        if not path.parts[-1].identifier:
            raise exceptions.NotFoundError(str(path))
        if len(path.parts) > 3:
            raise exceptions.NotFoundError(str(path))
        article_response = await self.make_request(
            'GET',
            self.build_url(path.is_public, *self.root_path_parts,
                           'articles', path.parts[1].identifier),
            expects=(200, 404),
        )
        if article_response.status == 404:
            raise exceptions.NotFoundError(str(path))
        article_json = await article_response.json()

        if len(path.parts) == 2:
            if article_json['defined_type'] not in pd_settings.FOLDER_TYPES:
                raise exceptions.NotFoundError(str(path))
            contents = []
            for file in article_json['files']:
                contents.append(FigshareFileMetadata(article_json, raw_file=file))
            return contents
        elif len(path.parts) == 3:
            for file in article_json['files']:
                if file['id'] == int(path.parts[2].identifier):
                    return FigshareFileMetadata(article_json, raw_file=file)
            raise exceptions.NotFoundError(path.path)
        else:
            raise exceptions.NotFoundError('{} is not a valid path.'.format(path))

    async def _get_article_metadata(self, article_id, is_public: bool):
        """Return Figshare*Metadata object for given article_id. Returns a FolderMetadata object
        for filesets, a FileMetadat object for other article types, and ``None`` if the article
        is not a fileset and has no files attached.

        Defined separately to allow for taking advantage of ``asyncio.gather``.

        :param str article_id: id of article whose metadata is requested
        :param bool is_public: ``True`` if article is accessed through public URN
        """
        response = await self.make_request(
            'GET',
            self.build_url(is_public, *self.root_path_parts, 'articles', article_id),
            expects=(200, ),
        )
        article_json = await response.json()
        if article_json['defined_type'] in pd_settings.FOLDER_TYPES:
            return FigshareFolderMetadata(article_json)
        elif article_json['files']:
            return FigshareFileMetadata(article_json)

        return None  # article without attached file

    async def _delete_container_contents(self):
        """Delete all articles within this Project or Collection."""
        # TODO: Needs logic for skipping public articles in collections
        articles = await self._get_all_articles()
        for article in articles:
            delete_article_response = await self.make_request(
                'DELETE',
                self.build_url(False, *self.root_path_parts, 'articles', str(article['id'])),
                expects=(204, ),
            )
            await delete_article_response.release()

    async def _get_all_articles(self):
        """Get all articles under a project or collection. This endpoint is paginated and does not
        provide limit metadata, so we keep querying until we receive an empty array response.
        See https://docs.figshare.com/api/#searching-filtering-and-pagination for details.

        :return: list of article json objects
        :rtype: `list`
        """
        all_articles, keep_going, page = [], True, 1
        while keep_going:
            resp = await self.make_request(
                'GET',
                self.build_url(False, *self.root_path_parts, 'articles'),
                params={'page': str(page), 'page_size': str(pd_settings.MAX_PAGE_SIZE)},
                expects=(200, ),
            )
            articles = await resp.json()
            all_articles.extend(articles)
            page += 1
            keep_going = len(articles) > 0

        return all_articles

    async def _create_article(self, data):
        """Create an article placeholder with the properties given in ``data``.  Returns the id of
        the new article. See https://docs.figshare.com/api/articles/#create-a-new-article for
        valid properties.

        :param dict data: properties to set for new article
        :return: the id of the newly created article
        :rtype: `str`
        """
        resp = await self.make_request(
            'POST',
            self.build_url(False, *self.root_path_parts, 'articles'),
            data=data,
            expects=(201, ),
            throws=exceptions.CreateFolderError,
        )
        articles_json = await resp.json()
        article_id = articles_json['location'].rsplit('/', 1)[1]
        return article_id


class FigshareArticleProvider(BaseFigshareProvider):

    def __init__(self, auth, credentials, settings, **kwargs):
        super().__init__(auth, credentials, settings, **kwargs)

    async def validate_v1_path(self, path, **kwargs):
        """Take a string path from the url and attempt to map it to an entity within this article.
        If the entity is found, returns a FigsharePath object with the entity identifiers included.
        Otherwise throws a 404 Not Found. Will also assert that the entity type inferred from the
        path matches the type of the entity at that url.

        :param str path: entity path from the v1 API
        :rtype FigsharePath:
        """
        if path == '/':
            return FigsharePath('/', _ids=('', ), folder=True, is_public=False)

        path_parts = self._path_split(path)
        if len(path_parts) != 2:
            raise exceptions.InvalidPathError('{} is not a valid Figshare path.'.format(path))

        file_id = path_parts[1]

        resp = await self.make_request(
            'GET',
            self.build_url(False, *self.root_path_parts, 'files', file_id),
            expects=(200, ),
        )
        file_json = await resp.json()
        return FigsharePath('/' + file_json['name'], _ids=('', file_id), folder=False,
                            is_public=False)

    async def validate_path(self, path, **kwargs):
        """Take a string path from the url and attempt to map it to an entity within this article.
        If the entity is found, returns a FigsharePath object with the entity identifiers included.
        Otherwise returns a FigsharePath with empty identifiers.

        :param str path: identifier path URN as passed through the v0 API
        :rtype FigsharePath:

        Quirks:

        * v0 may pass an identifier_path whose last part is a name and not an identifier, in the
          case of file/folder creation calls.

        * validate_path validates parent and returns a FigsharePath as accurately as possible.
        """
        if path == '/':
            return FigsharePath('/', _ids=('', ), folder=True, is_public=False)

        path_parts = self._path_split(path)
        if len(path_parts) != 2:
            raise exceptions.InvalidPathError('{} is not a valid Figshare path.'.format(path))

        file_id = path_parts[1]

        resp = await self.make_request(
            'GET',
            self.build_url(False, *self.root_path_parts, 'files', file_id),
            expects=(200, 404, ),
        )
        if resp.status == 200:
            file_json = await resp.json()
            file_name = file_json['name']
            return FigsharePath('/' + file_name, _ids=('', file_id), folder=False, is_public=False)

        # catch for create file in article root
        await resp.release()
        return FigsharePath('/' + file_id, _ids=('', ''), folder=False, is_public=False)

    async def revalidate_path(self, parent_path, child_name, folder: bool=False):
        r"""Attempt to get child's id and return FigsharePath of child.

        ``revalidate_path`` is used to check for the existance of a child_name/folder
        within the parent. Returning a FigsharePath of child. Child will have _id
        if conflicting child_name/folder exists otherwise _id will be ''.

        :param FigsharePath parent_path: Path of parent
        :param str child_name: Name of child
        :param bool folder: ``True`` if child is folder

        Code notes:

        Due to the fact that figshare allows duplicate titles/names for
        articles/files, revalidate_path can not be relied on to always return
        the correct id of an existing child_name. will return the first id that
        matches the folder and child_name arguments or '' if no match.
        """
        parent_is_folder = False
        urn_parts = self.root_path_parts
        if not parent_path.is_root:
            if folder:
                raise exceptions.NotFoundError(
                    '{} is not a valid parent path of folder={}. Folders can only exist at the '
                    'root level.'.format(parent_path.identifier_path, str(folder)))
            else:
                urn_parts = (*urn_parts, (parent_path.identifier))

        list_children_response = await self.make_request(
            'GET',
            self.build_url(False, *urn_parts),
            expects=(200, ),
        )

        child_id = ''
        article_json = await list_children_response.json()
        for file in article_json['files']:
            if file['name'] == child_name:
                child_id = str(file['id'])
                break

        return parent_path.child(child_name, _id=child_id, folder=folder,
                                 parent_is_folder=parent_is_folder)

    async def upload(self, stream, path, conflict='replace', **kwargs):
        r"""Upload a file to provider root or to an article whose defined_type is
        configured to represent a folder.

        :param asyncio.StreamReader stream: stream to upload
        :param FigsharePath path: FigsharePath to upload the file to.
        :param dict \*\*kwargs: Will be passed to returned metadata object
        """
        path, exists = await self.handle_name_conflict(path, conflict=conflict)
        if not path.parent.is_root:
            parent_resp = await self.make_request(
                'GET',
                self.build_url(False, *self.root_path_parts, 'articles', path.parent.identifier),
                expects=(200, ),
            )
            parent_json = await parent_resp.json()
            if not parent_json['defined_type'] in pd_settings.FOLDER_TYPES:
                del path._parts[1]

        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))
        file_id = await self._upload_file(self.container_id, path.name, stream)

        # Build new file path and return metadata
        path = FigsharePath('/' + file_id, _ids=('', file_id), folder=False, is_public=False)
        metadata = await self.metadata(path, **kwargs)
        if (
            (not metadata.extra['hashingInProgress']) and
            stream.writers['md5'].hexdigest != metadata.extra['hashes']['md5']
        ):
            raise exceptions.UploadChecksumMismatchError()

        return metadata, True

    async def create_folder(self, path, **kwargs):
        raise exceptions.CreateFolderError('Cannot create folders within articles.', code=400)

    async def delete(self, path, confirm_delete=0, **kwargs):
        """Delete the file at ``path``. If ``path`` is ``/`` and ``confirm_delete`` is ``1``, then
        delete all of the files within the article, but not the article itself.

        :param FigsharePath path: Path to be deleted
        :param int confirm_delete: Must be 1 to confirm root folder delete
        :rtype: None
        :raises: :class:`waterbutler.core.exceptions.NotFoundError`
        :raises: :class:`waterbutler.core.exceptions.DeleteError`

        Quirks:

        * If the FigsharePath given is for the provider root path, then the contents of the
          provider root path will be deleted, but not the provider root itself.
        """
        if path.is_root:
            if confirm_delete == 1:
                return await self._delete_container_contents()
            raise exceptions.DeleteError(
                'confirm_delete=1 is required for deleting root provider folder',
                code=400
            )

        await self._delete_file(path.parts[-1]._id)

    async def metadata(self, path, **kwargs):
        """Return metadata for entity identified by ``path``. May be the containing article or
        a file in a fileset article.

        :param FigsharePath path: entity whose metadata will be returned
        :rtype FigshareFileMetadata obj or list of Metadata objs:
        """
        article = await self._get_article(not path.is_public)
        if path.is_root:  # list files in article
            contents = []
            for file in article['files']:
                contents.append(FigshareFileMetadata(article, raw_file=file))
            return contents
        elif len(path.parts) == 2:  # metadata for a particular file
            for file in article['files']:
                if str(file['id']) == path.parts[1].identifier:
                    return FigshareFileMetadata(article, raw_file=file)

        # Invalid path, e.g. /422313/67709/1234
        raise exceptions.NotFoundError(str(path))

    async def _delete_container_contents(self):
        """Delete files within the containing article."""
        article = await self._get_article()
        for file in article['files']:
            await self._delete_file(str(file['id']))

    async def _get_article(self, is_owned=True):
        """Get the metadata for the container article. If the article is a public article not owned
        by the credentialed user, the request must be sent to a different endpoint.

        :param bool is_owned: Is this article owned by the credentialed user? Default: ``True``
        """
        resp = await self.make_request(
            'GET',
            self.build_url(not is_owned, *self.root_path_parts),
            expects=(200, ),
        )
        return await resp.json()

    async def _delete_file(self, file_id):
        """Delete a file from the root article. Docs:
        https://docs.figshare.com/api/articles/#delete-file-from-article

        :param str file: the id of the file to delete
        """
        resp = await self.make_request(
            'DELETE',
            self.build_url(False, *self.root_path_parts, 'files', file_id),
            expects=(204, ),
        )
        await resp.release()
