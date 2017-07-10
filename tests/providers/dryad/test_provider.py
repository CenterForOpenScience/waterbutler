from http import client

import pytest
import aiohttpretty

from waterbutler.core import exceptions
from waterbutler.providers.dryad import DryadProvider
from waterbutler.providers.dryad.path import DryadPath
from waterbutler.providers.dryad.metadata import DryadFileMetadata
from waterbutler.providers.dryad.metadata import DryadPackageMetadata
from waterbutler.providers.dryad.settings import DRYAD_META_URL, DRYAD_FILE_URL

from .fixtures import (package_scientific_metadata,
                       file_scientific_metadata,
                       file_system_metadata,
                       file_content)

@pytest.fixture
def auth():
    return {}

@pytest.fixture
def credentials():
    return {}

@pytest.fixture
def settings():
    return {'doi': '10.5061/dryad.1850'}

@pytest.fixture
def provider(auth, credentials, settings):
    return DryadProvider(auth, credentials, settings)


class TestValidatePath:

    @pytest.mark.asyncio
    async def test_validate_v1_path_root(self, provider, settings):
        path = await provider.validate_v1_path('/')
        assert path.path == ''
        assert not path.is_file
        assert path.is_dir
        assert path.is_root
        assert path.identifier == settings['doi']

    @pytest.mark.asyncio
    async def test_validate_path_root(self, provider, settings):
        path = await provider.validate_path('/')
        assert path.path == ''
        assert not path.is_file
        assert path.is_dir
        assert path.is_root
        assert path.identifier == settings['doi']

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_package(self, provider, package_scientific_metadata):

        package_id = provider.doi.split('.')[-1]
        package_name = 'Data from: Additional molecular support for the new chordate phylogeny'

        aiohttpretty.register_uri(
            'GET', '{}{}'.format(DRYAD_META_URL, package_id),
            body=package_scientific_metadata, headers={'Content-Type': 'application/xml'}
        )

        wb_path_v1 = await provider.validate_v1_path('/{}/'.format(package_id))
        assert wb_path_v1.package_id == package_id
        assert wb_path_v1.package_name == package_name
        assert wb_path_v1.name == package_name
        assert wb_path_v1.parent.name == ''
        assert not wb_path_v1.is_file
        assert wb_path_v1.is_dir
        assert not wb_path_v1.is_root

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/{}'.format(package_id))
        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/{}/'.format(package_id))

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, package_scientific_metadata, file_content):

        package_id = provider.doi.split('.')[-1]
        package_name = 'Data from: Additional molecular support for the new chordate phylogeny'

        file_id = '1'
        file_name = 'Delsuc2008-Genesis.nex'
        disposition = 'attachment; filename="{}"'.format(file_name)

        aiohttpretty.register_uri(
            'GET', '{}{}'.format(DRYAD_META_URL, package_id),
            body=package_scientific_metadata, headers={'Content-Type': 'application/xml'}
        )

        aiohttpretty.register_uri('GET',
                                  '{}{}/{}/bitstream'.format(DRYAD_META_URL, package_id, file_id),
                                  body=file_content,
                                  headers={'Content-Type': 'application/xml',
                                           'CONTENT-DISPOSITION': disposition})

        wb_path_v1 = await provider.validate_v1_path('/{}/{}'.format(package_id, file_id))
        assert wb_path_v1.name == file_name
        assert wb_path_v1.file_id == file_id
        assert wb_path_v1.file_name == file_name
        assert wb_path_v1.package_id == package_id
        assert wb_path_v1.package_name == package_name
        assert wb_path_v1.parent.name == package_name
        assert wb_path_v1.parent.identifier == package_id
        assert wb_path_v1.is_file
        assert not wb_path_v1.is_dir
        assert not wb_path_v1.is_root

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/{}/{}/'.format(package_id, file_id))
        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/{}/{}'.format(package_id, file_id))

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    async def test_reject_bogus_path(self, provider):
        bad_path = '/foo/bar/baz'
        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path(bad_path)
        assert exc.value.code == client.NOT_FOUND

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_path(bad_path)
        assert exc.value.code == client.NOT_FOUND


    @pytest.mark.asyncio
    async def test_mismatched_package_id(self, provider):
        mismatched_package = '/foo332/'
        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path(mismatched_package)
        assert exc.value.code == client.NOT_FOUND

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_path(mismatched_package)
        assert exc.value.code == client.NOT_FOUND


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_root(self, provider, settings, package_scientific_metadata):

        aiohttpretty.register_uri('GET', DRYAD_META_URL +provider.doi.split('.')[-1],
                                  body=package_scientific_metadata,
                                  headers={'Content-Type': 'application/xml'})

        path = DryadPath('/', [settings['doi']], folder=True)
        root_metadata = await provider.metadata(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_package(self, provider,
                                    package_scientific_metadata,
                                    file_scientific_metadata,
                                    file_system_metadata,
                                    file_content):

        package_id = provider.doi.split('.')[-1]
        file_id = '1'

        aiohttpretty.register_uri('GET', '{}{}'.format(DRYAD_META_URL, package_id),
                                  body=package_scientific_metadata,
                                  headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET', '{}{}/{}'.format(DRYAD_META_URL, package_id, file_id),
                                  body=file_scientific_metadata,
                                  headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET',
                                  '{}{}/{}/bitstream'.format(DRYAD_FILE_URL, package_id, file_id),
                                  body=file_system_metadata,
                                  headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET',
                                  '{}{}/{}/bitstream'.format(DRYAD_META_URL, package_id, file_id),
                                  body=file_content,
                                  headers={'Content-Type': 'application/xml',
                                           'CONTENT-DISPOSITION': 'attachment; filename="myfile.txt'})

        path = await provider.validate_v1_path('/{}/'.format(package_id))
        package_metadata = await provider.metadata(path)

        assert isinstance(package_metadata, list)
        assert len(package_metadata) == 1
        assert package_metadata[0].name == "myfile.txt"

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file(self, provider,
                                 package_scientific_metadata,
                                 file_scientific_metadata,
                                 file_system_metadata,
                                 file_content):

        package_id = provider.doi.split('.')[-1]
        file_id = '1'

        aiohttpretty.register_uri('GET', '{}{}'.format(DRYAD_META_URL, package_id),
                                  body=package_scientific_metadata,
                                  headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET', '{}{}/{}'.format(DRYAD_META_URL, package_id, file_id),
                                  body=file_scientific_metadata,
                                  headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET',
                                  '{}{}/{}/bitstream'.format(DRYAD_FILE_URL, package_id, file_id),
                                  body=file_system_metadata,
                                  headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET',
                                  '{}{}/{}/bitstream'.format(DRYAD_META_URL, package_id, file_id),
                                  body=file_content,
                                  headers={'Content-Type': 'application/xml',
                                           'CONTENT-DISPOSITION': 'attachment; filename="myfile.txt'})

        path = await provider.validate_v1_path('/{}/{}'.format(package_id, file_id))
        file_metadata = await provider.metadata(path)

        assert not isinstance(file_metadata, list)
        assert file_metadata.name == "myfile.txt"
        assert int(file_metadata.size) == 2874855


class TestDownload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download(self, provider,
                            package_scientific_metadata,
                            file_scientific_metadata,
                            file_system_metadata,
                            file_content):

        package_id = provider.doi.split('.')[-1]
        file_id = '1'
        file_name = 'Dsouli-InfectGenetEvol11.nex'

        aiohttpretty.register_uri('GET', '{}{}'.format(DRYAD_META_URL, package_id),
                                  body=package_scientific_metadata,
                                  headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET', '{}{}/{}'.format(DRYAD_META_URL, package_id, file_id),
                                  body=file_scientific_metadata,
                                  headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET',
                                  '{}{}/{}/bitstream'.format(DRYAD_FILE_URL, package_id, file_id),
                                  body=file_system_metadata,
                                  headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET',
                                  '{}{}/{}/bitstream'.format(DRYAD_META_URL, package_id, file_id),
                                  body=file_content,
                                  headers={'Content-Type': 'application/xml',
                                           'CONTENT-DISPOSITION': 'attachment; filename="{}"'.format(file_name)})

        path = await provider.validate_v1_path('/{}/{}'.format(package_id, file_id))
        result = await provider.download(path)
        content = await result.read()

        assert content == b"test_file_content"


class TestReadOnlyProvider:

    @pytest.mark.asyncio
    async def test_upload(self, provider):
        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
            await provider.upload('/foo-file.txt')
        assert e.value.code == 501

    @pytest.mark.asyncio
    async def test_delete(self, provider):
        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
            await provider.delete()
        assert e.value.code == 501

    @pytest.mark.asyncio
    async def test_move(self, provider):
        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
            await provider.move()
        assert e.value.code == 501

    @pytest.mark.asyncio
    async def test_copy_to(self, provider):
        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
            await provider.copy(provider)
        assert e.value.code == 501

    def test_can_intra_move(self, provider):
        assert provider.can_intra_move(provider) == False

    def test_can_intra_copy(self, provider):
        assert provider.can_intra_copy(provider) == False


# leftover bits
class TestMisc:

    def test_can_duplicate_name(self, provider):
        assert provider.can_duplicate_names() == False
