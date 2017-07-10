import pytest

from waterbutler.providers.dryad.path import DryadPath
from waterbutler.providers.dryad.settings import DRYAD_DOI_BASE

PACKAGE_ID = 'hs727'
FILE_ID = '1'
DOI = '{}{}'.format(DRYAD_DOI_BASE.replace('doi:', ''), PACKAGE_ID)

class TestBitbucketPath:

    def test_package(self):
        dryad_path = DryadPath('/FooBar/', _ids=[DOI, PACKAGE_ID], folder=True)
        assert dryad_path.package_id == PACKAGE_ID
        assert dryad_path.package_name == 'FooBar'
        assert dryad_path.is_package
        assert dryad_path.full_identifier == PACKAGE_ID
        assert dryad_path.materialized_path == '/FooBar/'


    def test_file(self):
        dryad_path = DryadPath('/FooBar/Baz', _ids=[DOI, PACKAGE_ID, FILE_ID], folder=False)
        assert dryad_path.package_id == PACKAGE_ID
        assert dryad_path.package_name == 'FooBar'
        assert dryad_path.file_id == FILE_ID
        assert dryad_path.file_name == 'Baz'
        assert not dryad_path.is_package
        assert dryad_path.full_identifier == '{}/{}'.format(PACKAGE_ID, FILE_ID)
        assert dryad_path.materialized_path == '/FooBar/Baz'
