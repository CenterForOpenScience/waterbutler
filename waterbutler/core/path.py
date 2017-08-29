import os
import typing  # noqa
import itertools

from waterbutler.core import metadata
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

    DECODE = lambda x: x  # type: typing.Callable[[str], str]
    ENCODE = lambda x: x  # type: typing.Callable[[str], str]

    def __init__(self, part: str, *, _id: str=None) -> None:
        self._id = _id
        self._count = 0  # type: int
        self._orig_id = _id
        self._orig_part = part
        self._name, self._ext = os.path.splitext(self.original_value)

    @property
    def identifier(self) -> str:
        return self._id

    @property
    def value(self) -> str:
        if self._count:
            return'{} ({}){}'.format(self._name, self._count, self._ext)
        return'{}{}'.format(self._name, self._ext)

    @property
    def raw(self) -> str:
        """ The `value` as passed through the `ENCODE` function"""
        return self.__class__.ENCODE(self.value)  # type: ignore

    @property
    def original_value(self) -> str:
        return self.__class__.DECODE(self._orig_part)  # type: ignore

    @property
    def original_raw(self) -> str:
        return self._orig_part

    @property
    def ext(self) -> str:
        return self._ext

    def increment_name(self, _id=None) -> 'WaterButlerPathPart':
        self._id = _id
        self._count += 1
        return self

    def renamed(self, name: str) -> 'WaterButlerPathPart':
        return self.__class__(self.__class__.ENCODE(name), _id=self._id)  # type: ignore

    def __repr__(self):
        return '{}({!r}, count={})'.format(self.__class__.__name__, self._orig_part, self._count)


class WaterButlerPath:
    """ A standardized and validated immutable WaterButler path.  This is our abstraction around
    file paths in storage providers.  A WaterButlerPath is an array of WaterButlerPathPart objects.
    Each PathPart has two important attributes, `value` and `_id`.  `value` is always the
    human-readable component of the path. If the provider assigns ids to entities (see: Box, Google
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

    Representations::

        path.path()
        path.raw_path()
        path.full_path()
        path.materialized_path()

    """

    PART_CLASS = WaterButlerPathPart

    @classmethod
    def generic_path_validation(cls, path: str) -> None:
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
    def validate_folder(cls, path: 'WaterButlerPath') -> None:
        if not path.is_dir:
            raise exceptions.CreateFolderError('Path must be a directory', code=400)

        if path.is_root:
            raise exceptions.CreateFolderError('Path can not be root', code=400)

    @classmethod
    def from_parts(cls,
                   parts: typing.Iterable[WaterButlerPathPart],
                   folder: bool=False,
                   **kwargs) -> 'WaterButlerPath':
        _ids, _parts = [], []
        for part in parts:
            _ids.append(part.identifier)
            _parts.append(part.raw)

        path = '/'.join(_parts)
        if parts and not path:
            path = '/'

        return cls(path, _ids=_ids, folder=folder, **kwargs)  # type: ignore

    @classmethod
    def from_metadata(cls,
                      path_metadata: metadata.BaseMetadata,
                      **kwargs):
        _ids = path_metadata.path.rstrip('/').split('/') or []
        return cls(path_metadata.materialized_path, _ids=_ids, folder=path_metadata.is_folder, **kwargs)  # type: ignore

    def __init__(self,
                 path: str,
                 _ids: typing.Sequence=(),
                 prepend: str=None,
                 folder: bool=None, **kwargs) -> None:
        # TODO: Should probably be a static method
        self.__class__.generic_path_validation(path)  # type: ignore

        self._orig_path = path

        self._prepend = prepend

        if prepend:
            self._prepend_parts = [self.PART_CLASS(part) for part in prepend.rstrip('/').split('/')]
        else:
            self._prepend_parts = []  # type: typing.List[WaterButlerPathPart]

        self._parts = [
            self.PART_CLASS(part, _id=_id)
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
    def is_root(self) -> bool:
        """ Returns `True` if the path is the root directory. """
        return len(self._parts) == 1

    @property
    def is_dir(self) -> bool:
        """ Returns `True` if the path represents a folder. """
        return self._is_folder

    @property
    def is_folder(self) -> bool:
        return self._is_folder

    @property
    def is_file(self) -> bool:
        """ Returns `True` if the path represents a file. """
        return not self._is_folder

    @property
    def kind(self) -> str:
        """ Returns `folder` if the path represents a folder, otherwise returns `file`. """
        return 'folder' if self._is_folder else 'file'

    @property
    def parts(self) -> list:
        """ Returns the list of WaterButlerPathParts that comprise this WaterButlerPath. """
        return self._parts

    @property
    def name(self) -> str:
        """ Returns the name of the file or folder. """
        return self._parts[-1].value

    @property
    def identifier(self):
        """ Returns the ID of the file or folder. """
        return self._parts[-1].identifier

    @property
    def identifier_path(self) -> str:
        """ Returns the ID formatted as a path for providers that use unique ids

        Quirk:
            If identifier is not set, raises TypeError
        """
        return '/' + self._parts[-1].identifier + ('/' if self.is_dir else '')

    @property
    def ext(self) -> str:
        """ Return the extension of the file """
        return self._parts[-1].ext

    @property
    def path(self) -> str:
        """ Returns a unix-style human readable path, relative to the provider storage root.
        Does NOT include a leading slash.  Calling `.path()` on the storage root returns the
        empty string.
        """
        if len(self.parts) == 1:
            return ''
        return '/'.join([x.value for x in self.parts[1:]]) + ('/' if self.is_dir else '')

    @property
    def raw_path(self) -> str:
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
    def materialized_path(self) -> str:
        """ Returns the user-readable unix-style path without the storage root prepended. """
        return '/'.join([x.value for x in self.parts]) + ('/' if self.is_dir else '')

    @property
    def parent(self):
        """ Returns a new WaterButlerPath that represents the parent of the current path.

        Calling `.parent()` on the root path returns None.
        """
        if len(self.parts) == 1:
            return None
        return self.__class__.from_parts(self.parts[:-1], folder=True, prepend=self._prepend)

    @property
    def extra(self) -> dict:
        """ Any extra provider-specific properties of the path. """
        return {}

    def child(self, name: str, _id=None, folder: bool=False):
        """ Create a child of the current WaterButlerPath, propagating prepend and id information to it.

        :param str name: the name of the child entity
        :param _id: the id of the child entity (defaults to None)
        :param bool folder: whether or not the child is a folder (defaults to False)
        """
        return self.__class__.from_parts(  # type: ignore
            self.parts + [self.PART_CLASS(name, _id=_id)],
            folder=folder, prepend=self._prepend
        )

    def increment_name(self) -> 'WaterButlerPath':
        self._parts[-1].increment_name()
        return self

    def rename(self, name) -> 'WaterButlerPath':
        self._parts[-1] = self._parts[-1].renamed(name)
        return self

    def __eq__(self, other):
        return isinstance(other, self.__class__) and str(self) == str(other)

    def __str__(self):
        return self.materialized_path

    def __repr__(self):
        return '{}({!r}, prepend={!r})'.format(self.__class__.__name__, self._orig_path, self._prepend)
