import aiohttpretty
import pytest
from http import client

from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.providers.dryad import DryadProvider
from waterbutler.providers.dryad.metadata import DryadPackageMetadata
from waterbutler.providers.dryad.metadata import DryadFileMetadata
from waterbutler.providers.dryad.settings import DRYAD_META_URL, DRYAD_FILE_URL

@pytest.fixture
def auth():
    return {}

@pytest.fixture
def credentials():
    return {}

@pytest.fixture
def settings():
    return {'doi': 'doi:10.5061/dryad.1850'}

@pytest.fixture
def provider(auth, credentials, settings):
    return DryadProvider(auth, credentials, settings)

@pytest.fixture
def object_package_metadata():
    return """
        <DryadDataPackage xmlns="http://purl.org/dryad/schema/terms/v3.1"
            xmlns:dwc="http://rs.tdwg.org/dwc/terms/"
            xmlns:dcterms="http://purl.org/dc/terms/"
            xmlns:bibo="http://purl.org/dryad/schema/dryad-bibo/v3.1"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://purl.org/dryad/schema/terms/v3.1
                http://datadryad.org/profile/v3.1/dryad.xsd">
          <dcterms:type>package</dcterms:type>
          <dcterms:creator>Delsuc, Frédéric</dcterms:creator>
          <dcterms:creator>Tsagkogeorga, Georgia</dcterms:creator>
          <dcterms:creator>Lartillot, Nicolas</dcterms:creator>
          <dcterms:creator>Philippe, Hervé</dcterms:creator>
          <dcterms:dateSubmitted>2010-08-10T13:17:46Z</dcterms:dateSubmitted>
          <dcterms:available>2010-08-10T13:17:46Z</dcterms:available>
          <dcterms:title>
            Data from: Additional molecular support for the new chordate phylogeny
          </dcterms:title>
          <dcterms:identifier>http://dx.doi.org/10.5061/dryad.1850</dcterms:identifier>
          <dcterms:description>
            Recent phylogenomic analyses have suggested tunicates instead of cephalochordates
             as the closest living relatives of vertebrates. In direct contradiction with the
             long accepted view of Euchordates, this new phylogenetic hypothesis for chordate
             evolution has been the object of some skepticism. We assembled an expanded
             phylogenomic dataset focused on deuterostomes. Maximum-likelihood using standard
             models and Bayesian phylogenetic analyses using the CAT site-heterogeneous mixture
             model of amino-acid replacement both provided unequivocal support for the
             sister-group relationship between tunicates and vertebrates (Olfactores). Chordates
             were recovered as monophyletic with cephalochordates as the most basal lineage.
             These results were robust to both gene sampling and missing data. New analyses of
             ribosomal rRNA also recovered Olfactores when compositional bias was alleviated.
             Despite the inclusion of 25 taxa representing all major lineages, the monophyly of
             deuterostomes remained poorly supported. The implications of these phylogenetic
             results for interpreting chordate evolution are discussed in light of recent
             advances from evolutionary developmental biology and genomics.
          </dcterms:description>
          <dcterms:subject>phylogenomics</dcterms:subject>
          <dcterms:subject>deuterostomes</dcterms:subject>
          <dcterms:subject>chordates</dcterms:subject>
          <dcterms:subject>tunicates</dcterms:subject>
          <dcterms:subject>cephalochordates</dcterms:subject>
          <dcterms:subject>olfactores</dcterms:subject>
          <dcterms:subject>ribosomal RNA</dcterms:subject>
          <dcterms:subject>jackknife</dcterms:subject>
          <dcterms:subject>evolution</dcterms:subject>
          <dwc:scientificName>Metazoa</dwc:scientificName>
          <dwc:scientificName>Deuterostomia</dwc:scientificName>
          <dwc:scientificName>Chordata</dwc:scientificName>
          <dwc:scientificName>Tunicata</dwc:scientificName>
          <dwc:scientificName>Urochordata</dwc:scientificName>
          <dwc:scientificName>Cephalochordata</dwc:scientificName>
          <dwc:scientificName>Hemichordata</dwc:scientificName>
          <dwc:scientificName>Xenoturbella</dwc:scientificName>
          <dwc:scientificName>Oikopleura</dwc:scientificName>
          <dwc:scientificName>Ciona</dwc:scientificName>
          <dwc:scientificName>Vertebrata</dwc:scientificName>
          <dwc:scientificName>Craniata</dwc:scientificName>
          <dwc:scientificName>Cyclostomata</dwc:scientificName>
          <dcterms:temporal>Phanerozoic</dcterms:temporal>
          <dcterms:references>http://dx.doi.org/10.1002/dvg.20450</dcterms:references>
          <dcterms:hasPart>http://dx.doi.org/10.5061/dryad.1850/1</dcterms:hasPart>
        </DryadDataPackage>
    """

@pytest.fixture
def object_file_metadata():
    return """
        <DryadDataFile xmlns="http://purl.org/dryad/schema/terms/v3.1"
            xmlns:dwc="http://rs.tdwg.org/dwc/terms/"
            xmlns:dcterms="http://purl.org/dc/terms/"
            xmlns:bibo="http://purl.org/dryad/schema/dryad-bibo/v3.1"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://purl.org/dryad/schema/terms/v3.1
                http://datadryad.org/profile/v3.1/dryad.xsd">
          <dcterms:type>file</dcterms:type>
          <dcterms:creator>Delsuc, Frédéric</dcterms:creator>
          <dcterms:creator>Tsagkogeorga, Georgia</dcterms:creator>
          <dcterms:creator>Lartillot, Nicolas</dcterms:creator>
          <dcterms:creator>Philippe, Hervé</dcterms:creator>
          <dcterms:title>Delsuc2008-Genesis.nex</dcterms:title>
          <dcterms:identifier>
            http://dx.doi.org/10.5061/dryad.1850/1
          </dcterms:identifier>
          <dcterms:rights>
            http://creativecommons.org/publicdomain/zero/1.0/
          </dcterms:rights>
          <dcterms:subject>phylogenomics</dcterms:subject>
          <dcterms:subject>deuterostomes</dcterms:subject>
          <dcterms:subject>chordates</dcterms:subject>
          <dcterms:subject>tunicates</dcterms:subject>
          <dcterms:subject>cephalochordates</dcterms:subject>
          <dcterms:subject>olfactores</dcterms:subject>
          <dcterms:subject>ribosomal RNA</dcterms:subject>
          <dcterms:subject>jackknife</dcterms:subject>
          <dcterms:subject>evolution</dcterms:subject>
          <dwc:scientificName>Metazoa</dwc:scientificName>
          <dwc:scientificName>Deuterostomia</dwc:scientificName>
          <dwc:scientificName>Chordata</dwc:scientificName>
          <dwc:scientificName>Tunicata</dwc:scientificName>
          <dwc:scientificName>Urochordata</dwc:scientificName>
          <dwc:scientificName>Cephalochordata</dwc:scientificName>
          <dwc:scientificName>Hemichordata</dwc:scientificName>
          <dwc:scientificName>Xenoturbella</dwc:scientificName>
          <dwc:scientificName>Oikopleura</dwc:scientificName>
          <dwc:scientificName>Ciona</dwc:scientificName>
          <dwc:scientificName>Vertebrata</dwc:scientificName>
          <dwc:scientificName>Craniata</dwc:scientificName>
          <dwc:scientificName>Cyclostomata</dwc:scientificName>
          <dcterms:temporal>Phanerozoic</dcterms:temporal>
          <dcterms:dateSubmitted>2010-08-10T13:17:40Z</dcterms:dateSubmitted>
          <dcterms:available>2010-08-10T13:17:40Z</dcterms:available>
          <dcterms:provenance>Made available in DSpace on 2010-08-10T13:17:40Z (GMT).
            No. of bitstreams: 1&#xD;
            Delsuc2008-Genesis.nex: 2874855 bytes, checksum:
            1ccd4f33cf0e67cdc859a2b969fd99bf (MD5)
          </dcterms:provenance>
          <dcterms:isPartOf>http://dx.doi.org/10.5061/dryad.1850</dcterms:isPartOf>
        </DryadDataFile>
"""

@pytest.fixture
def meta_file_metadata():
    return """<?xml version="1.0" encoding="UTF-8"?>
    <d1:systemMetadata xsi:schemaLocation="http://ns.dataone.org/service/types/v1
        http://ns.dataone.org/service/types/v1"
        xmlns:d1="http://ns.dataone.org/service/types/v1"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
      <serialVersion>1</serialVersion>
      <identifier>doi:10.5061/dryad.1850/1/bitstream</identifier>
      <formatId>text/plain</formatId>
      <size>2874855</size>
      <checksum algorithm="MD5">1ccd4f33cf0e67cdc859a2b969fd99bf</checksum>
      <submitter>bapat.amol@gmail.com</submitter>
      <rightsHolder>admin@datadryad.org</rightsHolder>
      <accessPolicy>
        <allow>
          <subject>public</subject>
          <permission>read</permission>
        </allow>
      </accessPolicy>
      <dateUploaded>2010-08-10T13:17:40Z</dateUploaded>
      <dateSysMetadataModified>2013-07-18T14:06:55.306-04:00</dateSysMetadataModified>
      <originMemberNode>urn:node:DRYAD</originMemberNode>
      <authoritativeMemberNode>urn:node:DRYAD</authoritativeMemberNode>
    </d1:systemMetadata>
    """

@pytest.fixture
def file_content():
    return b'test_file_content'


class TestValidatePath:
    """
        :note The methods, test_root, test_package and test_file will have to be removed when
        the v0 components of the provider are removed.
    """

    @pytest.mark.asyncio
    async def test_root(self, provider):
        path = await provider.validate_v1_path('/')
        assert path.path == ''
        assert not path.is_file
        assert path.is_dir
        assert path.is_root

    @pytest.mark.asyncio
    async def test_package(self, provider):
        stripped_doi = provider.doi.split('.')[-1]
        path = await provider.validate_v1_path('/'+stripped_doi+'/')
        assert path.name == '1850'
        assert path.parent.name == ''
        assert not path.is_file
        assert path.is_dir
        assert not path.is_root

    @pytest.mark.asyncio
    async def test_file(self, provider):
        stripped_doi = provider.doi.split('.')[-1]
        path = await provider.validate_path('/'+stripped_doi+"/1")
        assert path.name == '1'
        assert path.parent.name == '1850'
        assert path.is_file
        assert not path.is_dir
        assert not path.is_root

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_package(self, provider, object_package_metadata):
        aiohttpretty.register_uri(
            'GET', DRYAD_META_URL + provider.doi.split('.')[-1],
            body=object_package_metadata, headers={'Content-Type': 'application/xml'}
        )
        aiohttpretty.register_uri(
            'GET', DRYAD_META_URL + 'XXXX',
            status=404
        )

        stripped_doi = provider.doi.split('.')[-1]
        wb_path_v1 = await provider.validate_v1_path('/' + stripped_doi + '/')

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/XXXX')
        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + stripped_doi + '/')

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, object_file_metadata):

        aiohttpretty.register_uri(
            'GET', DRYAD_META_URL +  provider.doi.split('.')[-1] + "/1",
            body=object_file_metadata, headers={'Content-Type': 'application/xml'}
        )
        aiohttpretty.register_uri(
            'GET', DRYAD_META_URL +  provider.doi.split('.')[-1] + "/9999",
            status=404
        )
        stripped_doi = provider.doi.split('.')[-1]
        wb_path_v1 = await provider.validate_v1_path('/' + stripped_doi + "/1")

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + stripped_doi + "/9999")
        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + stripped_doi + '/1')

        assert wb_path_v1 == wb_path_v0


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_root(self, provider, object_package_metadata):

        aiohttpretty.register_uri('GET', DRYAD_META_URL +provider.doi.split('.')[-1],
                                  body=object_package_metadata,
                                  headers={'Content-Type': 'application/xml'})

        path = WaterButlerPath('/')
        root_metadata = await provider.metadata(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_package(self, provider,
                                object_package_metadata,
                                object_file_metadata,
                                meta_file_metadata,
                                file_content):
        aiohttpretty.register_uri('GET', DRYAD_META_URL + provider.doi.split('.')[-1],
                              body=object_package_metadata,
                              headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET', DRYAD_META_URL  + provider.doi.split('.')[-1]+'/',
                              body=object_package_metadata,
                              headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET', DRYAD_META_URL + provider.doi.split('.')[-1]+"/1",
                              body=object_file_metadata,
                              headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET', DRYAD_FILE_URL + provider.doi.split('.')[-1]+"/1/bitstream",
                              body=meta_file_metadata,
                              headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET', DRYAD_META_URL + provider.doi.split('.')[-1]+"/1/bitstream",
                              body=file_content,
                              headers={'Content-Type': 'application/xml',
                                        'CONTENT-DISPOSITION': 'attachment; filename="myfile.txt'})

        stripped_doi = provider.doi.split('.')[-1]
        path = await provider.validate_v1_path('/' + stripped_doi + '/')
        package_metadata = await provider.metadata(path)

        assert isinstance(package_metadata, list)
        assert len(package_metadata) == 1
        assert package_metadata[0].name == "myfile.txt"

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file(self, provider,
                                object_package_metadata,
                                object_file_metadata,
                                meta_file_metadata,
                                file_content):
        aiohttpretty.register_uri('GET', DRYAD_META_URL + provider.doi.split('.')[-1],
                              body=object_package_metadata,
                              headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET', DRYAD_META_URL+ provider.doi.split('.')[-1]+"/1",
                              body=object_file_metadata,
                              headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET', DRYAD_FILE_URL+ provider.doi.split('.')[-1]+"/1/bitstream",
                              body=meta_file_metadata,
                              headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET', DRYAD_META_URL+ provider.doi.split('.')[-1]+"/1/bitstream",
                              body=file_content,
                              headers={'Content-Type': 'application/xml',
                                        'CONTENT-DISPOSITION': 'attachment; filename="myfile.txt'})
        stripped_doi = provider.doi.split('.')[-1]
        path = await provider.validate_v1_path('/' + stripped_doi + '/1')
        file_metadata = await provider.metadata(path)

        assert not isinstance(file_metadata, list)
        assert file_metadata.name == "myfile.txt"
        assert int(file_metadata.size) == 2874855


class TestDownload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download(self, provider,
                                object_package_metadata,
                                object_file_metadata,
                                meta_file_metadata,
                                file_content):

        aiohttpretty.register_uri('GET', DRYAD_META_URL + provider.doi.split('.')[-1],
                              body=object_package_metadata,
                              headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET', DRYAD_META_URL + provider.doi.split('.')[-1]+"/1",
                              body=object_file_metadata,
                              headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET', DRYAD_FILE_URL + provider.doi.split('.')[-1]+"/1/bitstream",
                              body=meta_file_metadata,
                              headers={'Content-Type': 'application/xml'})
        aiohttpretty.register_uri('GET', DRYAD_META_URL + provider.doi.split('.')[-1]+"/1/bitstream",
                              body=file_content,
                              headers={'Content-Type': 'application/xml',
                                        'CONTENT-DISPOSITION': 'attachment; filename="Dsouli-InfectGenetEvol11.nex'})
        stripped_doi = provider.doi.split('.')[-1]
        path = await provider.validate_v1_path('/' + stripped_doi + '/1')
        result = await provider.download(path)
        content = await result.read()

        assert content == b"test_file_content"
