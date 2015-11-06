import abc
import hashlib

import furl

from waterbutler.server import settings


class BaseMetadata(metaclass=abc.ABCMeta):
    """The BaseMetadata object provides structure
    for all metadata returned via WaterButler
    """

    def __init__(self, raw):
        self.raw = raw

    def serialized(self):
        """The JSON serialization of metadata from WaterButler.
        .. warning::
        This method determines the output of API v0
        """
        return {
            'extra': self.extra,
            'kind': self.kind,
            'name': self.name,
            'path': self.path,
            'provider': self.provider,
            'materialized': self.materialized_path,
            'etag': hashlib.sha256('{}::{}'.format(self.provider, self.etag).encode('utf-8')).hexdigest(),
        }

    def json_api_serialized(self, resource):
        """The JSON API serialization of metadata from WaterButler.
        .. warning::
        This method determines the output of API v1
        """
        return {
            'id': self.provider + self.path,
            'type': 'files',
            'attributes': self.serialized(),
            'links': self._json_api_links(resource),
        }

    def _json_api_links(self, resource):
        entity_url = self._entity_url(resource)
        actions = {
            'move': entity_url,
            'upload': entity_url + '?kind=file',
            'delete': entity_url,
        }

        return actions

    def _entity_url(self, resource):
        url = furl.furl(settings.DOMAIN)
        segments = ['v1', 'resources', resource, 'providers', self.provider]
        # If self is a folder, path ends with a slash which must be preserved. However, furl
        # percent-encodes the trailing slash. Instead, turn folders into a list of (path_id, ''),
        # and let furl add the slash for us.  The [1:] is because path always begins with a slash,
        # meaning the first entry is always ''.
        segments += self.path.split('/')[1:]
        url.path.segments.extend(segments)

        return url.url

    def build_path(self, path):
        if not path.startswith('/'):
            path = '/' + path
        if self.kind == 'folder' and not path.endswith('/'):
            path += '/'
        return path

    @property
    def is_folder(self):
        return self.kind == 'folder'

    @property
    def is_file(self):
        return self.kind == 'file'

    @abc.abstractproperty
    def provider(self):
        """The provider from which this resource
        originated.
        """
        raise NotImplementedError

    @abc.abstractproperty
    def kind(self):
        """`file` or `folder`"""
        raise NotImplementedError

    @abc.abstractproperty
    def name(self):
        """The name to show a users
        ::
            /bar/foo.txt -> foo.txt
            /<someid> -> whatever.png
        """
        raise NotImplementedError

    @abc.abstractproperty
    def path(self):
        """The canonical string representation
        of a waterbutler file or folder.

        ..note::
            All paths MUST start with a `/`
            All Folders MUST end with a `/`
        """
        raise NotImplementedError

    @property
    def materialized_path(self):
        """The "pretty" variant of path
        this path can be displayed to the enduser

        path -> /Folder%20Name/123abc
        full_path -> /Folder Name/File Name

        ..note::
            All paths MUST start with a `/`
            All Folders MUST end with a `/`
        ..note::
            Defaults to self.path
        """
        return self.path

    @property
    def extra(self):
        return {}

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.serialized() == other.serialized()


class BaseFileMetadata(BaseMetadata):

    def serialized(self):
        return dict(super().serialized(), **{
            'contentType': self.content_type,
            'modified': self.modified,
            'size': self.size,
        })

    def _json_api_links(self, resource):
        ret = super()._json_api_links(resource)
        ret['download'] = self._entity_url(resource)
        return ret

    @property
    def kind(self):
        """File"""
        return 'file'

    @abc.abstractproperty
    def content_type(self):
        raise NotImplementedError

    @abc.abstractproperty
    def modified(self):
        raise NotImplementedError

    @abc.abstractproperty
    def size(self):
        raise NotImplementedError

    @property
    def etag(self):
        raise NotImplementedError


class BaseFileRevisionMetadata(metaclass=abc.ABCMeta):

    def __init__(self, raw):
        self.raw = raw

    def serialized(self):
        return {
            'extra': self.extra,
            'version': self.version,
            'modified': self.modified,
            'versionIdentifier': self.version_identifier,
        }

    def json_api_serialized(self):
        """The JSON API serialization of revision metadata from WaterButler.
        .. warning::
        This method determines the output of API v1
        """
        return {
            'id': self.version,
            'type': 'file_versions',
            'attributes': self.serialized(),
        }

    @abc.abstractproperty
    def modified(self):
        raise NotImplementedError

    @abc.abstractproperty
    def version(self):
        raise NotImplementedError

    @abc.abstractproperty
    def version_identifier(self):
        raise NotImplementedError

    @property
    def extra(self):
        return {}

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.serialized() == other.serialized()


class BaseFolderMetadata(BaseMetadata):
    """Defines that metadata structure for
    folders, auto defines :func:`kind`
    """

    def __init__(self, raw):
        super().__init__(raw)
        self._children = None

    def serialized(self):
        ret = super().serialized()
        if self.children is not None:
            ret['children'] = [c.serialized() for c in self.children]
        return ret

    def json_api_serialized(self, resource):
        ret = super().json_api_serialized(resource)
        ret['attributes']['size'] = None
        return ret

    def _json_api_links(self, resource):
        ret = super()._json_api_links(resource)
        ret['new_folder'] = self._entity_url(resource) + '?kind=folder'
        return ret

    @property
    def children(self):
        return self._children

    @children.setter
    def children(self, kids):
        self._children = kids

    @property
    def kind(self):
        return 'folder'

    @property
    def etag(self):
        return None
