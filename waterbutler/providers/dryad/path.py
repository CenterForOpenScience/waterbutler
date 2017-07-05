from waterbutler.core import path


class DryadPath(path.WaterButlerPath):
    """Dryad paths consist of two parts, the package doi and the file id within the doi.
    """

    @property
    def package_id(self):
        return self.parts[1].identifier

    @property
    def package_name(self):
        return self.parts[1].value

    @property
    def file_id(self):
        return self.parts[2].identifier

    @property
    def file_name(self):
        return self.parts[2].value

    @property
    def is_package(self):
        return len(self.parts) < 3

    @property
    def full_identifier(self):
        if self.is_package:
            return self.package_id
        return '{}/{}'.format(self.package_id, self.file_id)
