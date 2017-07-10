from waterbutler.core import utils
from waterbutler.core import metadata

from .settings import DRYAD_DOI_BASE
from .utils import get_xml_element, get_xml_element_list


class BaseDryadMetadata(metadata.BaseMetadata):
    """ Translation from Dryad Metadata format to Waterbutler Metadata
    """

    def __init__(self, path, science_meta):
        """Base class for Package and File Metadata classes.

        :param xml.dom.minidom raw: Source metadata from Dryad API. Must be parseable by `xml.dom.minidom`
        :param str doi: Dryad DOI. Format: doi:10.5061/dryad.XXXX
        """
        super().__init__({})
        self._path = path
        self._science_meta = science_meta
        self.dryad_doi = '{}{}'.format(DRYAD_DOI_BASE, self._path.package_id)

    @property
    def path(self):
        return '/' + self._path.full_identifier + ('/' if self._path.is_dir else '')

    @property
    def name(self):
        return self._path.name

    @property
    def materialized_path(self):
        return self._path.materialized_path

    @property
    def id(self):
        return self._path.identifier

    @property
    def provider(self):
        return 'dryad'

    @property
    def extra(self):
        return {'doi': self.dryad_doi,
                'spatial': get_xml_element(self._science_meta, 'dcterms:spatial'),
                'available': get_xml_element(self._science_meta, 'dcterms:available'),
                'scientificName': get_xml_element_list(self._science_meta, 'dwc:scientificName'),
                'subject': get_xml_element_list(self._science_meta, 'dcterms:subject'),
                'description': get_xml_element(self._science_meta, 'dcterms:description'),
                'rights': get_xml_element(self._science_meta, 'dcterms:rights'),
                'id': get_xml_element(self._science_meta, 'dcterms:identifier'),
                'creators': get_xml_element_list(self._science_meta, 'dcterms:creator')}

    @property
    def etag(self):
        return '{}::{}'.format(self.name, self.dryad_doi)

    def _json_api_links(self, resource):
        """Removes the delete, upload, and new_folder links (provider is read-only)."""
        links = super()._json_api_links(resource)
        for action in ['delete', 'upload', 'new_folder']:
            if action in links:
                links[action] = None
        return links


class DryadFileMetadata(BaseDryadMetadata, metadata.BaseFileMetadata):
    """
        DryadFileMetadata needs to be instantiated with the same metadata from
        the raw API call that normal packages do, AND with metadata from the file
        metadata API and information that is only found in the bit stream.
    """
    def __init__(self, path, science_meta, system_meta):
        BaseDryadMetadata.__init__(self, path, science_meta)
        self._system_meta = system_meta

    @property
    def modified(self):
        return get_xml_element(self._science_meta, 'dcterms:dateSubmitted')

    @property
    def created_utc(self):
        return utils.normalize_datetime(get_xml_element(self._science_meta, 'dcterms:dateSubmitted'))

    @property
    def content_type(self):
        return get_xml_element(self._system_meta, 'formatId')

    @property
    def size(self):
        size = get_xml_element(self._system_meta, 'size')
        return None if size is None else int(size)

    @property
    def extra(self):
        extra = super().extra
        extra.update({
            'part_of': get_xml_element(self._science_meta, 'dcterms:isPartOf')
        })
        return extra


class DryadPackageMetadata(BaseDryadMetadata, metadata.BaseFolderMetadata):

    @property
    def content_type(self):
        return get_xml_element(self._science_meta, 'dcterms:type')

    @property
    def file_parts(self):
        return get_xml_element_list(self._science_meta, 'dcterms:hasPart')

    @property
    def extra(self):
        extra = super().extra
        extra.update({
            'references': get_xml_element(self._science_meta, 'dcterms:references'),
            'file_parts': self.file_parts
        })
        return extra


class DryadFileRevisionMetadata(metadata.BaseFileRevisionMetadata):

    def __init__(self, raw, science_meta):
        super().__init__(raw)
        self._science_meta = science_meta

    @property
    def version(self):
        return 'latest'

    @property
    def version_identifier(self):
        return 'version'

    @property
    def modified(self):
        return get_xml_element(self._science_meta, 'dcterms:dateSubmitted')
