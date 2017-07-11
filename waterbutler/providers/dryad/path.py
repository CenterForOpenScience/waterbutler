from waterbutler.core import path


class DryadPath(path.WaterButlerPath):
    """Dryad paths consist of one, two, or three parts.

    * One part: this path represents the storage root. As currently implemented, the Dryad addon
      only supports one configured package per provider.  The "value" of the root is always the
      empty string, and its "identifier" is the DOI of the currently connected package (e.g.
      ``doi:10.5061/dryad.XXXX``)

    * Two parts: this path represents a single package.  The "value" is the name of the package.
      The "identifier" is the unique suffix of the DOI.  This is currently the root identifier
      without the ``doi:10.5061/dryad.`` leading string.

    * Three parts: this path represents a file within a package.  The "value" is the name of the
      file. The "identifier" is the index number of the file within the package.
    """

    @property
    def package_id(self):
        """The ID of the package. The suffix of the DOI."""
        return self.parts[1].identifier

    @property
    def package_name(self):
        """The name of the package"""
        return self.parts[1].value

    @property
    def file_id(self):
        """The ID of the file.  Its index within the package."""
        return self.parts[2].identifier

    @property
    def file_name(self):
        """The name of the file."""
        return self.parts[2].value

    @property
    def is_package(self):
        """Whether or not this Path represents a package."""
        return len(self.parts) == 2

    @property
    def full_identifier(self):
        if self.is_package:
            return self.package_id
        return '{}/{}'.format(self.package_id, self.file_id)
