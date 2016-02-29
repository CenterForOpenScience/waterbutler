import os
import itertools
from waterbutler.core import exceptions


class WaterButlerPathPart:
    """ A WaterButlerPathPart represents one level of a unix-style path.  For instance,
    `/foo/bar/baz.txt` would be composed of four PathParts: the root, `foo`, `bar`, and `baz.txt`.
    Each PathPart has a `value`, an `_id`, a `count`, and an `ext`.  The `value` is the
    human-readable component of the path, such as `baz.txt` or `bar`.  The `_id` is the unique
    identifier for the PathPart.  This is optional.  Some providers use unique identifiers, others
    do not.  The `count` property is used for Mac-style renaming, where `(1)` is appended to a path
    name when a copy operation encounters a naming conflict.  `ext` is inferred from the initial
    path.
    """

    DECODE = lambda x: x
    ENCODE = lambda x: x

    def __init__(self, part, _id=None):
        self._id = _id
        self._count = 0
        self._orig_id = _id
        self._orig_part = part
        self._name, self._ext = os.path.splitext(self.original_value)

    @property
    def identifier(self):
        return self._id

    @property
    def value(self):
        if self._count:
            return'{} ({}){}'.format(self._name, self._count, self._ext)
        return'{}{}'.format(self._name, self._ext)

    @property
    def raw(self):
        """ The `value` as passed through the `ENCODE` function"""
        return self.__class__.ENCODE(self.value)

    @property
    def original_value(self):
        return self.__class__.DECODE(self._orig_part)

    @property
    def original_raw(self):
        return self._orig_part

    @property
    def ext(self):
        return self._ext

    def increment_name(self, _id=None):
        self._id = _id
        self._count += 1
        return self

    def renamed(self, name):
        return self.__class__(self.__class__.ENCODE(name), _id=self._id)

    def __repr__(self):
        return '{}({!r}, count={})'.format(self.__class__.__name__, self._orig_part, self._count)


class WaterButlerPath:
    """ A standardized and validated immutable WaterButler path.  This is our abstraction around
    file paths in storage providers.  A WaterButlerPath is an array of WaterButlerPathPart objects.
    Each PathPart has two important attributes, `value` and `_id`.  `value` is always the
    human-readble component of the path. If the provider assigns ids to entities (see: Box, Google
    Drive, OSFStorage), that id belongs in the `_id` attribute. If `/Foo/Bar/baz.txt` is stored on
    Box, its path parts will be approximately::

        [
          { value: '/',       _id: None, }, # must have root
          { value: 'Foo',     _id: '1112192435' },
          { value: 'Bar',     _id: '1112202348' },
          { value: 'baz.txt', _id: '1113345897' },
        ]

    If :func:`WaterButlerPath.identifier` is called on this object, it'll return the `_id` of the
    last path part. :func:`WaterButlerPath.path` will return `/Foo/Bar/baz.txt`.

    A valid WaterButlerPath should always have a root path part.

    Some providers, such as Google Drive, require paths to be encoded when used in URLs.
    WaterButlerPathPart has `ENCODE` and `DECODE` class methods that handle this. The encoded path
    is available through the `.raw_path()` method.

    To get a human-readable materialized path, call `str()` on the WaterButlerPath.
    """

    PART_CLASS = WaterButlerPathPart

    @classmethod
    def generic_path_validation(cls, path):
        """Validates a WaterButler specific path, e.g. /folder/file.txt, /folder/
        :param str path: WaterButler path
        """
        if not path:
            raise exceptions.InvalidPathError('Must specify path')
        if not path.startswith('/'):
            raise exceptions.InvalidPathError('Invalid path \'{}\' specified'.format(path))
        if '//' in path:
            raise exceptions.InvalidPathError('Invalid path \'{}\' specified'.format(path))
        # Do not allow path manipulation via shortcuts, e.g. '..'
        absolute_path = os.path.abspath(path)
        if not path == '/' and path.endswith('/'):
            absolute_path += '/'
        if not path == absolute_path:
            raise exceptions.InvalidPathError('Invalid path \'{}\' specified'.format(absolute_path))

    @classmethod
    def validate_folder(cls, path):
        if not path.is_dir:
            raise exceptions.CreateFolderError('Path must be a directory', code=400)

        if path.is_root:
            raise exceptions.CreateFolderError('Path can not be root', code=400)

    @classmethod
    def from_parts(cls, parts, folder=False, **kwargs):
        _ids, _parts = [], []
        for part in parts:
            _ids.append(part.identifier)
            _parts.append(part.raw)

        path = '/'.join(_parts)
        if parts and not path:
            path = '/'

        return cls(path, _ids=_ids, folder=folder, **kwargs)

    def __init__(self, path, _ids=(), prepend=None, folder=None):
        self.__class__.generic_path_validation(path)

        self._orig_path = path

        self._prepend = prepend

        if prepend:
            self._prepend_parts = [self.PART_CLASS(part, None) for part in prepend.rstrip('/').split('/')]
        else:
            self._prepend_parts = []

        self._parts = [
            self.PART_CLASS(part, _id)
            for _id, part in
            itertools.zip_longest(_ids, path.rstrip('/').split('/'))
        ]

        if folder is not None:
            self._is_folder = bool(folder)
        else:
            self._is_folder = self._orig_path.endswith('/')

        if self.is_dir and not self._orig_path.endswith('/'):
            self._orig_path += '/'

    @property
    def is_root(self):
        """ Returns `True` if the path is the root directory. """
        return len(self._parts) == 1

    @property
    def is_dir(self):
        """ Returns `True` if the path represents a folder. """
        return self._is_folder

    @property
    def kind(self):
        """ Returns `folder` if the path represents a folder, otherwise returns `file`. """
        return 'folder' if self._is_folder else 'file'

    @property
    def is_file(self):
        """ Returns `True` if the path represents a file. """
        return not self._is_folder

    @property
    def parts(self):
        """ Returns the list of WaterButlerPathParts that comprise this WaterButlerPath. """
        return self._parts

    @property
    def name(self):
        """ Returns the name of the file or folder. """
        return self._parts[-1].value

    @property
    def identifier(self):
        """ Returns the ID of the file or folder. """
        return self._parts[-1].identifier

    @property
    def identifier_path(self):
        """ Returns the ID formatted as a path for providers that use unique ids

        Quirk:
            If identifier is not set raises TypeError
        """
        return '/' + self._parts[-1].identifier + ('/' if self.is_dir else '')

    @property
    def ext(self):
        """ Return the extension of the file """
        return self._parts[-1].ext

    @property
    def path(self):
        """ Returns a unix-style human readable path, relative to the provider storage root.
        Does NOT include a leading slash.  Calling `.path()` on the storage root returns the
        empty string.
        """
        if len(self.parts) == 1:
            return ''
        return '/'.join([x.value for x in self.parts[1:]]) + ('/' if self.is_dir else '')

    @property
    def raw_path(self):
        """ Like `.path()`, but passes each path segment through the PathPart's ENCODE function.
        """
        if len(self.parts) == 1:
            return ''
        return '/'.join([x.raw for x in self.parts[1:]]) + ('/' if self.is_dir else '')

    @property
    def full_path(self):
        """ Same as `.path()`, but with the provider storage root prepended. """
        return '/'.join([x.value for x in self._prepend_parts + self.parts[1:]]) + ('/' if self.is_dir else '')

    @property
    def parent(self):
        """ Returns a new WaterButlerPath that represents the parent of the current path.

        Calling `.parent()` on the root path returns None.
        """
        if len(self.parts) == 1:
            return None
        return self.__class__.from_parts(self.parts[:-1], folder=True, prepend=self._prepend)

    def child(self, name, _id=None, folder=False):
        """ Create a child of the current WaterButlerPath, propagating prepend and id information to it.

        :param str name: the name of the child entity
        :param _id: the id of the child entity (defaults to None)
        :param bool folder: whether or not the child is a folder (defaults to False)
        """
        return self.__class__.from_parts(self.parts + [self.PART_CLASS(name, _id=_id)], folder=folder, prepend=self._prepend)

    def increment_name(self):
        self._parts[-1].increment_name()
        return self

    def rename(self, name):
        self._parts[-1] = self._parts[-1].renamed(name)
        return self

    def __eq__(self, other):
        return isinstance(other, self.__class__) and str(self) == str(other)

    def __str__(self):
        """ Returns the materialized path """
        return '/'.join([x.value for x in self.parts]) + ('/' if self.is_dir else '')

    def __repr__(self):
        return '{}({!r}, prepend={!r})'.format(self.__class__.__name__, self._orig_path, self._prepend)
