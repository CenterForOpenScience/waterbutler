import xml.dom.minidom

import pytest

from waterbutler.providers.dryad.path import DryadPath
from waterbutler.providers.dryad.settings import DRYAD_DOI_BASE
from waterbutler.providers.dryad.metadata import DryadFileMetadata
from waterbutler.providers.dryad.metadata import DryadPackageMetadata
from waterbutler.providers.dryad.metadata import DryadFileRevisionMetadata

from .fixtures import (package_scientific_metadata,
                       file_scientific_metadata,
                       file_system_metadata,
                       file_content)

PACKAGE_ID = 'hs727'
PACKAGE_NAME = 'Data from: Additional molecular support for the new chordate phylogeny'

FILE_ID = '1'
FILE_NAME = 'Delsuc2008-Genesis.nex'

DOI = '{}{}'.format(DRYAD_DOI_BASE.replace('doi:', ''), PACKAGE_ID)


class TestDryadMetadata:

    def test_build_file_metadata(self, file_scientific_metadata, file_system_metadata):
        full_path = '/{}/{}'.format(PACKAGE_ID, FILE_ID)
        full_name = '/{}/{}'.format(PACKAGE_NAME, FILE_NAME)

        path = DryadPath(full_name, _ids=[DOI, PACKAGE_ID, FILE_ID], folder=False)
        science_meta = xml.dom.minidom.parseString(file_scientific_metadata)
        system_meta = xml.dom.minidom.parseString(file_system_metadata)

        try:
            metadata = DryadFileMetadata(path, science_meta, system_meta)
        except Exception as exc:
            pytest.fail(str(exc))

        assert metadata.name == FILE_NAME
        assert metadata.path == full_path
        assert metadata.materialized_path == full_name
        assert metadata.id == FILE_ID
        assert metadata.kind == 'file'
        assert metadata.modified == '2010-08-10T13:17:40Z'
        assert metadata.modified_utc == '2010-08-10T13:17:40+00:00'
        assert metadata.created_utc == '2010-08-10T13:17:40+00:00'
        assert metadata.content_type == 'text/plain'
        assert metadata.size == 2874855
        assert metadata.etag == '{}::{}'.format(
            FILE_NAME,
            '{}{}'.format(DRYAD_DOI_BASE, PACKAGE_ID)
        )
        assert metadata.provider == 'dryad'

        # web_view = ('https://bitbucket.org/{}/{}/src/{}{}?'
        #             'fileviewer=file-view-default'.format(owner, repo, COMMIT_SHA, full_path))
        # assert metadata.web_view == web_view
        # assert metadata.extra == {
        #     'webView': web_view,
        # }

        resource = 'mst3k'
        action_url = 'http://localhost:7777/v1/resources/{}/providers/dryad{}'.format(resource, full_path)
        assert metadata._json_api_links(resource) == {
            'delete': None,
            'upload': None,
            'move': action_url,
            'download': action_url,
        }

    def test_build_folder_metadata(self, package_scientific_metadata):
        full_path = '/{}/'.format(PACKAGE_ID)
        full_name = '/{}/'.format(PACKAGE_NAME)

        path = DryadPath(full_name, _ids=[DOI, PACKAGE_ID], folder=True)
        science_meta = xml.dom.minidom.parseString(package_scientific_metadata)

        try:
            metadata = DryadPackageMetadata(path, science_meta)
        except Exception as exc:
            pytest.fail(str(exc))

        assert metadata.name == PACKAGE_NAME
        assert metadata.path == full_path
        assert metadata.materialized_path == full_name
        assert metadata.id == PACKAGE_ID
        assert metadata.kind == 'folder'
        assert metadata.children is None
        # assert metadata.extra == {}
        assert metadata.provider == 'dryad'

        assert metadata._json_api_links('mst3k') == {
            'delete': None,
            'upload': None,
            'move': 'http://localhost:7777/v1/resources/mst3k/providers/dryad{}'.format(full_path),
            'new_folder': None,
        }

    def test_build_revision_metadata(self, file_scientific_metadata):

        science_meta = xml.dom.minidom.parseString(file_scientific_metadata)
        try:
            metadata = DryadFileRevisionMetadata({}, science_meta)
        except Exception as exc:
            pytest.fail(str(exc))

        assert metadata.version == 'latest'
        assert metadata.version_identifier == 'version'
        assert metadata.modified == '2010-08-10T13:17:40Z'
        assert metadata.modified_utc == '2010-08-10T13:17:40+00:00'
        assert metadata.extra == {}
        assert metadata.serialized() == {
            'extra': {},
            'version': 'latest',
            'modified': '2010-08-10T13:17:40Z',
            'modified_utc': '2010-08-10T13:17:40+00:00',
            'versionIdentifier': 'version',
        }
