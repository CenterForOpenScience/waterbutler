import abc
import typing
import hashlib

import furl

from waterbutler.core import utils
from waterbutler.server import settings


class BaseMetadata(metaclass=abc.ABCMeta):
    """The BaseMetadata object provides the base structure for all metadata returned via
    WaterButler.  It also implements the API serialization methods to turn metadata objects
    into primitive data structures suitable for serializing.

    The basic metadata response looks like::

        {
          "path": "",
          "name": "",
          "kind": "",
          "provider": "",
          "materialized": "",
          "provider": "",
          "etag": "",
          "extra": {}
        }
    """

    def __init__(self, raw: dict) -> None:
        self.raw = raw

    def serialized(self) -> dict:
        """Returns a dict of primitives suitable for serializing into JSON.

        .. note::

            This method determines the output of API v0 and v1.

        :rtype: dict
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

    def json_api_serialized(self, resource: str) -> dict:
        """Returns a dict of primitives suitable for serializing into a JSON-API -compliant
        response.  Sets the `id` and `type` attributes required by JSON-API, and stores the
        metadata under the `attributes` key.  A `links` object provides a dict of actions and the
        urls where those actions can be performed.

        .. note::

            This method determines the output of API v1.

        :rtype: dict
        """
        json_api = {
            'id': self.provider + self.path,
            'type': 'files',
            'attributes': self.serialized(),
            'links': self._json_api_links(resource),
        }
        # Typing: skip "unsupported target for indexed assignment" errors for nested dict from method
        json_api['attributes']['resource'] = resource  # type: ignore
        return json_api

    def _json_api_links(self, resource: str) -> dict:
        """ Returns a dict of action names and the endpoints where those actions are performed.

        :rtype: dict
        """
        entity_url = self._entity_url(resource)
        actions = {
            'move': entity_url,
            'upload': entity_url + '?kind=file',
            'delete': entity_url,
        }

        return actions

    def _entity_url(self, resource: str) -> str:
        """ Utility method for constructing the base url for actions. """
        url = furl.furl(settings.DOMAIN)
        segments = ['v1', 'resources', resource, 'providers', self.provider]
        # If self is a folder, path ends with a slash which must be preserved. However, furl
        # percent-encodes the trailing slash. Instead, turn folders into a list of (path_id, ''),
        # and let furl add the slash for us.  The [1:] is because path always begins with a slash,
        # meaning the first entry is always ''.
        segments += self.path.split('/')[1:]
        url.path.segments.extend(segments)

        return url.url

    def build_path(self, path) -> str:
        if not path.startswith('/'):
            path = '/' + path
        if self.kind == 'folder' and not path.endswith('/'):
            path += '/'
        return path

    @property
    def is_folder(self) -> bool:
        """ Does this object describe a folder?

        :rtype: bool
        """
        return self.kind == 'folder'

    @property
    def is_file(self) -> bool:
        """ Does this object describe a file?

        :rtype: bool
        """
        return self.kind == 'file'

    @property
    @abc.abstractmethod
    def provider(self) -> str:
        """ The provider from which this resource originated. """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def kind(self) -> str:
        """ `file` or `folder` """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """ The user-facing name of the entity, excluding parent folder(s).
        ::

            /bar/foo.txt -> foo.txt
            /<someid> -> whatever.png
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def path(self) -> str:
        """ The canonical string representation of a waterbutler file or folder.  For providers
        that track entities with unique IDs, this will be the ID.  For providers that do not, this
        will usually be the full unix-style path of the file or folder.

        .. note::

            All paths MUST start with a `/`
            All folder entities MUST end with a `/`
            File entities MUST never end with a `/`
        """
        raise NotImplementedError

    @property
    def etag(self) -> str:
        raise NotImplementedError

    @property
    def materialized_path(self) -> str:
        """ The unix-style path of the file relative the the root of the provider.  Encoded
        entities should be decoded.

        e.g.::

            path              -> /313c57f9a9edeb87139b205beaed
            name              -> Foo.txt
            materialized_path -> /Parent Folder/Foo.txt

        .. note::

            All materialized_paths MUST start with a `/`
            All folder entities MUST end with a `/`
            File entities MUST never end with a `/`

        .. note::

            Defaults to `self.path`
        """
        return self.path

    @property
    def extra(self) -> dict:
        """A dict of optional metadata from the provider.  Non-mandatory metadata should be stored
        in this property.

        While this field is not explicitly structured, the `hashes` key should be reserved for the
        following.  If the provider supplies MD5 or SHA256 hashes, those should be saved in a dict
        under the `hashes` key, with `md5`, `sha256` as the canonical key names for the hashes.
        """
        return {}

    def __eq__(self, other: 'BaseMetadata') -> bool:  # type: ignore
        return isinstance(other, self.__class__) and self.serialized() == other.serialized()


class BaseFileMetadata(BaseMetadata):
    """ The base class for WaterButler metadata objects for files.  In addition to the properties
    required by `BaseMetadata`, `BaseFileMetadata` requires the consumer to implement the
    `content_type`, `modified`, and `size` properties.  The `etag` may be added in a subclass.
    """

    def serialized(self) -> dict:
        """ Returns a dict representing the file's metadata suitable to be serialized into JSON.

        :rtype: dict
        """
        return dict(super().serialized(), **{
            'contentType': self.content_type,
            'modified': self.modified,
            'modified_utc': self.modified_utc,
            'created_utc': self.created_utc,
            'size': self.size,
            'sizeInt': self.size_as_int,
        })

    def _json_api_links(self, resource: str) -> dict:
        """ Adds the `download` link to the JSON-API repsonse `links` field.

        :rtype: dict
        """
        ret = super()._json_api_links(resource)
        ret['download'] = self._entity_url(resource)
        return ret

    @property
    def kind(self) -> str:
        """ File objects have `kind == 'file'` """
        return 'file'

    @property
    @abc.abstractmethod
    def content_type(self) -> str:
        """ MIME-type of the file (if available) """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def modified(self) -> str:
        """ Date the file was last modified, as reported by the provider, in
        the format used by the provider. """
        raise NotImplementedError

    @property
    def modified_utc(self) -> str:
        """ Date the file was last modified, as reported by the provider,
        converted to UTC, in format (YYYY-MM-DDTHH:MM:SS+00:00). """
        return utils.normalize_datetime(self.modified)

    @property
    def created_utc(self) -> str:
        """ Date the file was created, as reported by the provider,
        converted to UTC, in format (YYYY-MM-DDTHH:MM:SS+00:00). """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def size(self) -> typing.Union[int, str]:
        """ Size of the file in bytes. Should be a int, but some providers return a string and WB
        never casted it.  The `size_as_int` property was added to enforce this without breaking
        exisiting code and workarounds.
        """
        raise NotImplementedError

    @property
    def size_as_int(self) -> int:
        """ Size of the file in bytes.  Always an `int` or `None`. Some providers report size as a
        `str`. Both exist to maintain backwards compatibility.
        """
        try:
            size_as_int = int(self.size)
        except (TypeError, ValueError):
            size_as_int = None
        return size_as_int


class BaseFileRevisionMetadata(metaclass=abc.ABCMeta):

    def __init__(self, raw: dict) -> None:
        self.raw = raw

    def serialized(self) -> dict:
        return {
            'extra': self.extra,
            'version': self.version,
            'modified': self.modified,
            'modified_utc': self.modified_utc,
            'versionIdentifier': self.version_identifier,
        }

    def json_api_serialized(self) -> dict:
        """The JSON API serialization of revision metadata from WaterButler.

        .. note::

            This method determines the output of API v1
        """
        return {
            'id': self.version,
            'type': 'file_versions',
            'attributes': self.serialized(),
        }

    @property
    @abc.abstractmethod
    def modified(self) -> str:
        raise NotImplementedError

    @property
    def modified_utc(self) -> str:
        """ Date the revision was last modified, as reported by the provider,
        converted to UTC, in format (YYYY-MM-DDTHH:MM:SS+00:00). """
        return utils.normalize_datetime(self.modified)

    @property
    @abc.abstractmethod
    def version(self) -> str:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def version_identifier(self) -> str:
        raise NotImplementedError

    @property
    def extra(self) -> dict:
        return {}

    def __eq__(self, other: 'BaseFileRevisionMetadata') -> bool:  # type: ignore
        return isinstance(other, self.__class__) and self.serialized() == other.serialized()


class BaseFolderMetadata(BaseMetadata):
    """ Defines the metadata structure for folders, auto defines :func:`kind`.   In addition
    to the properties required by `BaseMetadata`, `BaseFolderMetadata` does not require any
    additional properties beyond those required by `BaseMetadata`.  Provides an `etag` property
    that defaults to `None`. It also extends `BaseMetadata` to provide an accessor/mutator for
    children, which should be a list of file metadata objects that inherit from `BaseFileMetadata`.
    """

    def __init__(self, raw: dict) -> None:
        super().__init__(raw)
        self._children = None  # type: list

    def serialized(self) -> dict:
        """ Returns a dict representing the folder's metadata suitable to be serialized
        into JSON. If the `children` property has not been set, it will be excluded from
        the dict.

        :rtype: dict
        """
        ret = super().serialized()
        if self.children is not None:
            ret['children'] = [c.serialized() for c in self.children]
        return ret

    def json_api_serialized(self, resource: str) -> dict:
        """ Return a JSON-API compliant serializable dict, suitable for the WB v1 API.  Sets the
        `size` attribute to `None`, as folders do no have a size.

        :rtype: dict
        """
        ret = super().json_api_serialized(resource)
        ret['attributes']['size'] = None
        ret['attributes']['sizeInt'] = None
        return ret

    def _json_api_links(self, resource: str) -> dict:
        """ Adds the `new_folder` link to the JSON-API repsonse `links` field.

        :rtype: dict
        """
        ret = super()._json_api_links(resource)
        ret['new_folder'] = self._entity_url(resource) + '?kind=folder'
        return ret

    @property
    def children(self) -> typing.List[BaseMetadata]:
        """ (Optional) A list of child entities of the folder.  Each entity should be either a
        file or folder metadata object.  Will be `None` if the presence of children is unknown.

        :rtype: None or list of Metadata objects
        """
        return self._children

    @children.setter
    def children(self, kids: typing.List[BaseMetadata]):
        """ Assigns the given list to the children property.  The affirmative absence of child
        entities should be indicated by passing an empty list.

        :param list kids: list of children of the folder.
        """
        self._children = kids

    @property
    def kind(self) -> str:
        """ Folder metadata objects have `kind == 'folder'` """
        return 'folder'

    @property
    def etag(self) -> typing.Union[str, None]:
        """ FIXME: An etag? """
        return None
