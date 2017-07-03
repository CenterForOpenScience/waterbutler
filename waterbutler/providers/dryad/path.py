from waterbutler.core import path


class DryadPath(path.WaterButlerPath):
    """Dryad paths consist of two parts, the package doi and the file id within the doi.
    """

    @property
    def package_doi(self):
        return self.parts[1].value

    @property
    def file_id(self):
        return self.parts[2].value

    @property
    def is_package(self):
        return len(self.parts) < 3
