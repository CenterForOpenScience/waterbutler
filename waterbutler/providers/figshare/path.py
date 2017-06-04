from waterbutler.core.path import WaterButlerPath


class FigsharePath(WaterButlerPath):

    def __init__(self, path,
                 folder: bool,
                 is_public=False,
                 parent_is_folder=True,
                 _ids=(),
                 prepend=None) -> None:
        super().__init__(path, _ids=_ids, prepend=prepend, folder=folder)
        self.is_public = is_public
        self.parent_is_folder = parent_is_folder

    @property
    def identifier_path(self):
        """Returns a path based on article/file identifiers, relative to the provider storage root.
        Does NOT include a leading slash.  Calling ``.identifier_path()`` on the storage root
        returns the empty string.
        """
        if len(self.parts) == 1:
            return ''
        return '/'.join([x.identifier for x in self.parts[1:]]) + ('/' if self.is_dir else '')

    @property
    def parent(self):
        """ Returns a new WaterButlerPath that represents the parent of the current path.

        Calling `.parent()` on the root path returns None.
        """
        if len(self.parts) == 1:
            return None
        return self.__class__.from_parts(self.parts[:-1], folder=self.parent_is_folder,
                                         is_public=self.is_public, prepend=self._prepend)

    def child(self, name, _id=None, folder=False, parent_is_folder=True):
        """ Create a child of the current WaterButlerPath, propagating prepend and id information to it.

        :param str name: the name of the child entity
        :param _id: the id of the child entity (defaults to None)
        :param bool folder: whether or not the child is a folder (defaults to False)
        """
        return self.__class__.from_parts(
            self.parts + [self.PART_CLASS(name, _id=_id)],
            folder=folder, parent_is_folder=parent_is_folder,
            prepend=self._prepend
        )
