import abc
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

    def __init__(self, raw):
        self.raw = raw

    def serialized(self):
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

    def json_api_serialized(self, resource):
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
        json_api['attributes']['resource'] = resource
        return json_api

    def _json_api_links(self, resource):
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

    def _entity_url(self, resource):
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

    def build_path(self, path):
        if not path.startswith('/'):
            path = '/' + path
        if self.kind == 'folder' and not path.endswith('/'):
            path += '/'
        return path

    @property
    def is_folder(self):
        """ Does this object describe a folder?

        :rtype: bool
        """
        return self.kind == 'folder'

    @property
    def is_file(self):
        """ Does this object describe a file?

        :rtype: bool
        """
        return self.kind == 'file'

    @abc.abstractproperty
    def provider(self):
        """ The provider from which this resource originated. """
        raise NotImplementedError

    @abc.abstractproperty
    def kind(self):
        """ `file` or `folder` """
        raise NotImplementedError

    @abc.abstractproperty
    def name(self):
        """ The user-facing name of the entity, excluding parent folder(s).
        ::

            /bar/foo.txt -> foo.txt
            /<someid> -> whatever.png
        """
        raise NotImplementedError

    @abc.abstractproperty
    def path(self):
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
    def materialized_path(self):
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
    def extra(self):
        """A dict of optional metdata from the provider.  Non-mandatory metadata should be stored
        in this property.

        While this field is not explicitly structured, the `hashes` key should be reserved for the
        following.  If the provider supplies MD5 or SHA256 hashes, those should be saved in a dict
        under the `hashes` key, with `md5`, `sha256` as the canonical key names for the hashes.
        """
        return {}

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.serialized() == other.serialized()


class BaseFileMetadata(BaseMetadata):
    """ The base class for WaterButler metadata objects for files.  In addition to the properties
    required by `BaseMetadata`, `BaseFileMetadata` requires the consumer to implement the
    `content_type`, `modified`, and `size` properties.  The `etag` may be added, but defaults to `None`.
    """

    def serialized(self):
        """ Returns a dict representing the file's metadata suitable to be serialized into JSON.

        :rtype: dict
        """
        return dict(super().serialized(), **{
            'contentType': self.content_type,
            'modified': self.modified,
            'modified_utc': self.modified_utc,
            'created_utc': self.created_utc,
            'size': self.size,
        })

    def _json_api_links(self, resource):
        """ Adds the `download` link to the JSON-API repsonse `links` field.

        :rtype: dict
        """
        ret = super()._json_api_links(resource)
        ret['download'] = self._entity_url(resource)
        return ret

    @property
    def kind(self):
        """ File objects have `kind == 'file'` """
        return 'file'

    @abc.abstractproperty
    def content_type(self):
        """ MIME-type of the file (if available) """
        raise NotImplementedError

    @abc.abstractproperty
    def modified(self):
        """ Date the file was last modified, as reported by the provider, in
        the format used by the provider. """
        raise NotImplementedError

    @property
    def modified_utc(self):
        """ Date the file was last modified, as reported by the provider,
        converted to UTC, in format (YYYY-MM-DDTHH:MM:SS+00:00). """
        return utils.normalize_datetime(self.modified)

    @property
    def created_utc(self):
        """ Date the file was created, as reported by the provider,
        converted to UTC, in format (YYYY-MM-DDTHH:MM:SS+00:00). """
        raise NotImplementedError

    @abc.abstractproperty
    def size(self):
        """ Size of the file in bytes. """
        raise NotImplementedError

    @property
    def etag(self):
        """ FIXME: An etag? """
        raise NotImplementedError


class BaseFileRevisionMetadata(metaclass=abc.ABCMeta):

    def __init__(self, raw):
        self.raw = raw

    def serialized(self):
        return {
            'extra': self.extra,
            'version': self.version,
            'modified': self.modified,
            'modified_utc': self.modified_utc,
            'versionIdentifier': self.version_identifier,
        }

    def json_api_serialized(self):
        """The JSON API serialization of revision metadata from WaterButler.

        .. note::

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

    @property
    def modified_utc(self):
        """ Date the revision was last modified, as reported by the provider,
        converted to UTC, in format (YYYY-MM-DDTHH:MM:SS+00:00). """
        return utils.normalize_datetime(self.modified)

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
    """ Defines the metadata structure for folders, auto defines :func:`kind`.   In addition
    to the properties required by `BaseMetadata`, `BaseFolderMetadata` does not require any
    additional properties beyond those required by `BaseMetadata`.  Provides an `etag` property
    that defaults to `None`. It also extends `BaseMetadata` to provide an accessor/mutator for
    children, which should be a list of file metadata objects that inherit from `BaseFileMetadata`.
    """

    def __init__(self, raw):
        super().__init__(raw)
        self._children = None

    def serialized(self):
        """ Returns a dict representing the folder's metadata suitable to be serialized
        into JSON. If the `children` property has not been set, it will be excluded from
        the dict.

        :rtype: dict
        """
        ret = super().serialized()
        if self.children is not None:
            ret['children'] = [c.serialized() for c in self.children]
        return ret

    def json_api_serialized(self, resource):
        """ Return a JSON-API compliant serializable dict, suitable for the WB v1 API.  Sets the
        `size` attribute to `None`, as folders do no have a size.

        :rtype: dict
        """
        ret = super().json_api_serialized(resource)
        ret['attributes']['size'] = None
        return ret

    def _json_api_links(self, resource):
        """ Adds the `new_folder` link to the JSON-API repsonse `links` field.

        :rtype: dict
        """
        ret = super()._json_api_links(resource)
        ret['new_folder'] = self._entity_url(resource) + '?kind=folder'
        return ret

    @property
    def children(self):
        """ (Optional) A list of child entities of the folder.  Each entity should be either a
        file or folder metadata object.  Will be `None` if the presence of children is unknown.

        :rtype: None or list of Metdata objects
        """
        return self._children

    @children.setter
    def children(self, kids):
        """ Assigns the given list to the children property.  The affirmative absence of child
        entities should be indicated by passing an empty list.

        :param list kids: list of children of the folder.
        """
        self._children = kids

    @property
    def kind(self):
        """ Folder metadata objects have `kind == 'folder'` """
        return 'folder'

    @property
    def etag(self):
        """ FIXME: An etag? """
        return None
