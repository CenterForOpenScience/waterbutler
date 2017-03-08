import aiohttp
import mimetypes

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.fedora import settings
from waterbutler.providers.fedora.metadata import FedoraFileMetadata
from waterbutler.providers.fedora.metadata import FedoraFolderMetadata

# User specifies a Fedora 4 repo which they have access to through HTTP basic authentication.
#
# Credentials:
#   repo:     URL to Fedora 4 repository
#   user:     Username for repo
#   password: Password for repo
#
# Provider written against Fedora 4.7.1


class FedoraProvider(provider.BaseProvider):
    """Provider for a Fedora 4 repository.

    This provider uses the Fedora 4 REST API which is based on LDP.

    API docs: https://wiki.duraspace.org/display/FEDORA4x/Fedora+4.x+Documentation

    Quirks:
    * Fedora 4 is a component, not a singular service like Dropbox or Box.
    """

    NAME = 'fedora'

    def __init__(self, auth, credentials, prov_settings):
        super().__init__(auth, credentials, prov_settings)
        self.repo = self.credentials['repo']
        self.basic_auth_token = aiohttp.BasicAuth(self.credentials['user'], self.credentials['password']).encode()

    def build_repo_url(self, path, **query):
        """Return Fedora url for resource identified by WaterButlerPath"""
        segments = [s.original_raw for s in path.parts]
        return provider.build_url(self.repo, *segments, **query)

    async def validate_v1_path(self, path, **kwargs):
        """Return WaterButlerPath for a path string.

        Ensure that if the path is to a folder, it corresponds to a Fedora container
        and otherwise corresponds to a Fedora binary. Fedora resource must also exist.

        Throw NotFoundError if resource does not exist or types do not match
        """

        wb_path = WaterButlerPath(path)
        url = self.build_repo_url(wb_path)

        is_container = await self.is_fedora_container(url)

        if is_container == wb_path.is_dir:
            return wb_path

        raise exceptions.NotFoundError(str(path))

    async def validate_path(self, path, **kwargs):
        """ Return WaterButlerPath for a path string."""
        return WaterButlerPath(path)

    def can_duplicate_names(self):
        return False

    # Copy and move only supported within this provider.

    def can_intra_move(self, other, path=None):
        return self == other

    def can_intra_copy(self, other, path=None):
        return self == other

    def fedora_url_to_path(self, url):
        """Transform a url in the fedora repo to a WaterButlerPath"""
        return WaterButlerPath('/' + url[len(self.repo):].strip('/'))

    async def path_exists(self, path):
        """ Return whether or not a path exists """

        # The exists method returns metadata object for a file and
        # a list of metadata objects for a folder.
        md = await self.exists(path)

        if isinstance(md, list):
            return True
        if isinstance(md, FedoraFileMetadata):
            return True
        return False

    async def intra_copy(self, dest_provider, src_path, dest_path):
        """Copies src_path to dest_path.

        Returns BaseMetadata, Success tuple.

        """

        # Must delete existing destination
        exists = await self.path_exists(dest_path)

        if exists:
            await self.delete(dest_path)

        src_url = self.build_repo_url(src_path)
        dest_url = self.build_repo_url(dest_path)

        async with self.request(
            'COPY', src_url,
            headers={'Destination': dest_url},
            expects=(201,),
            throws=exceptions.IntraCopyError
        ) as resp:
            # Recalcuate destination path based on Location header
            dest_path = self.fedora_url_to_path(resp.headers.get('Location'))

            md = await self.lookup_fedora_metadata(dest_path)

            return md, not exists

    async def intra_move(self, dest_provider, src_path, dest_path):
        """Moves src_path to dest_path.

        Returns BaseMetadata, Success tuple.
        """

        # Must delete existing destination
        exists = await self.path_exists(dest_path)

        if exists:
            await self.delete(dest_path)

        src_url = self.build_repo_url(src_path)
        dest_url = self.build_repo_url(dest_path)

        async with self.request(
            'MOVE', src_url,
            headers={'Destination': dest_url},
            expects=(201,),
            throws=exceptions.IntraMoveError
        ) as move_resp:
            # Delete tombstone of original file
            async with self.request('DELETE', src_url + '/fcr:tombstone', expects=(204, ), throws=exceptions.DeleteError):
                pass

            # Recalcuate destination path based on Location header
            dest_path = self.fedora_url_to_path(move_resp.headers.get('Location'))

            md = await self.lookup_fedora_metadata(dest_path)
            return md, not exists

    @property
    def default_headers(self):
        return {
            'Authorization': self.basic_auth_token
        }

    async def download(self, path, revision=None, range=None, **kwargs):
        """Download a Fedora binary"""

        url = self.build_repo_url(path)

        resp = await self.make_request(
            'GET',
            url,
            range=range,
            expects=(200,),
            throws=exceptions.DownloadError,
        )

        return streams.ResponseStreamReader(resp)

    async def upload(self, stream, path, conflict='replace', **kwargs):
        """Create a Fedora binary corrsponding to the path and return FedoraFileMetadata for it"""

        path, exists = await self.handle_name_conflict(path, conflict=conflict)
        url = self.build_repo_url(path)

        # Must provide a Content-Type. Otherwise a container is created.
        mime_type, encoding = mimetypes.guess_type(url)

        if mime_type is None:
            mime_type = settings.OCTET_STREAM_MIME_TYPE

        # Must not say content is RDF because a container will be created.
        if mime_type in settings.RDF_MIME_TYPES:
            mime_type = settings.OCTET_STREAM_MIME_TYPE

        async with self.request(
            'PUT',
            url,
            headers={'Content-Length': str(stream.size), 'Content-Type': mime_type},
            data=stream,
            expects=(201, 204),
            throws=exceptions.UploadError,
        ):
            md = await self.metadata(path)
            return md, not exists

    async def delete(self, path, confirm_delete=0, **kwargs):
        """Delete the Fedora resource corrsponding to the path"""

        if path.is_root:
            if confirm_delete == 1:
                await self.delete_folder_contents(path)
                return
            else:
                raise exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=400
                )

        url = self.build_repo_url(path)

        async with self.request(
            'DELETE', url,
            expects=(204, ),
            throws=exceptions.DeleteError,
        ):
            pass

        # Must delete the tombstone so the resource can be recreated.
        async with self.request(
            'DELETE', url + '/fcr:tombstone',
            expects=(204, ),
            throws=exceptions.DeleteError,
        ):
            pass

    async def delete_folder_contents(self, path, **kwargs):
        """Delete the contents of a folder. For use against provider root.

        :param WaterButlerPath path: WaterButlerPath path object for folder
        """

        meta = (await self.metadata(path))
        for child in meta:
            child_path = await self.validate_path(child.path)
            await self.delete(child_path)

    async def metadata(self, path, revision=None, **kwargs):
        """Given a WaterBulterPath, return metadata about the specified resource.

        The JSON-LD representations of Fedora resources are parsed as simple JSON.
        This is a little brittle and may cause issues in the future.

        """

        result = await self.lookup_fedora_metadata(path)

        # If fedora resource is container, return list of metadata about child resources.

        return result.list_children_metadata(self.repo) if result.is_folder else result

    async def lookup_fedora_metadata(self, path):
        """Return FedoraFileMetadata for a Fedora binary or FedoraFolderMetadata for a container.

        Must do a HEAD request to figure out how to retrieve metadata because the url to a Fedora binary
        resource must have /fcr:metadata appended to it.
        """

        fedora_id = self.build_repo_url(path)
        is_container = await self.is_fedora_container(fedora_id)

        url = fedora_id + ('' if is_container else '/fcr:metadata')

        # The Prefer header tells fedora to include triples for child resources.

        async with self.request(
            'GET', url,
            headers={'Accept': 'application/ld+json',
                     'Prefer': 'return=representation; include="' + settings.EMBED_RESOURCES_URI + '"'},
            expects=(200, 404),
            throws=exceptions.MetadataError
        ) as resp:

            if resp.status == 404:
                raise exceptions.NotFoundError(str(path))

            raw = await resp.json()

            if is_container:
                return FedoraFolderMetadata(raw, fedora_id, path)
            else:
                return FedoraFileMetadata(raw, fedora_id, path)

    async def is_fedora_container(self, url):
        """Do a head request on a url to check if it is a fedora container"""

        async with self.request(
            'HEAD', url,
            expects=(200, 404),
            throws=exceptions.MetadataError
        ) as resp:
            if resp.status == 404:
                raise exceptions.NotFoundError(str(url))

            return settings.LDP_CONTAINER_TYPE_HEADER in resp.headers.getall('Link', [])

    async def create_folder(self, path, folder_precheck=True, **kwargs):
        """Create the specified folder as a Fedora container and return FedoraFolderMetadata for it"""

        WaterButlerPath.validate_folder(path)

        url = self.build_repo_url(path)

        async with self.request(
            'PUT',
            url,
            headers={'Content-Type': 'text/turtle'},
            expects=(201,),
            throws=exceptions.CreateFolderError
        ):
            pass

        return await self.lookup_fedora_metadata(path)
