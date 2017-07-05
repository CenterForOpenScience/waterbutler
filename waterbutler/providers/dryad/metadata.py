from waterbutler.core import utils
from waterbutler.core import metadata

from .settings import DRYAD_DOI_BASE


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

    def _get_element(self, name):
        """Helper function for retrieving metadata fields from source document by name. Returns
        first element found.

        :param str name: Element tag.
        :rtype: str
        :return: String contents of element or None
        """
        el = self._science_meta.getElementsByTagName(name)
        if len(el) > 0:
            return el[0].firstChild.wholeText
        else:
            return None  # TESTME

    def _get_element_list(self, name):
        """Helper function for retrieving metadata fields from source document by name.

        :param str name: Element tag.
        :rtype: str
        :return: string contents of element
        """
        return [i.firstChild.wholeText for i in self._science_meta.getElementsByTagName(name)]

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
                'spatial': self._get_element('dcterms:spatial'),
                'available': self._get_element('dcterms:available'),
                'scientificName': self._get_element_list('dwc:scientificName'),
                'subject': self._get_element_list('dcterms:subject'),
                'description': self._get_element('dcterms:description'),
                'rights': self._get_element('dcterms:rights'),
                'id': self._get_element('dcterms:identifier'),
                'creators': self._get_element_list('dcterms:creator')}

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

    def _get_file_element(self, name):
        el = self._system_meta.getElementsByTagName(name)
        if len(el) > 0:
            return el[0].firstChild.wholeText
        else:
            return None

    def _get_file_element_list(self, name):
        return [i.firstChild.wholeText for i in self._system_meta.getElementsByTagName(name)]

    @property
    def modified(self):
        return self._get_element('dcterms:dateSubmitted')

    @property
    def created_utc(self):
        return utils.normalize_datetime(self._get_element('dcterms:dateSubmitted'))

    @property
    def content_type(self):
        return self._get_file_element('formatId')

    @property
    def size(self):
        size = self._get_file_element('size')
        return None if size is None else int(size)

    @property
    def extra(self):
        extra = super().extra
        extra.update({
            'part_of': self._get_element('dcterms:isPartOf')
        })
        return extra


class DryadPackageMetadata(BaseDryadMetadata, metadata.BaseFolderMetadata):

    @property
    def content_type(self):
        return self._get_element('dcterms:type')

    @property
    def file_parts(self):
        return self._get_element_list('dcterms:hasPart')

    @property
    def extra(self):
        extra = super().extra
        extra.update({
            'references': self._get_element('dcterms:references'),
            'file_parts': self.file_parts
        })
        return extra


class DryadFileRevisionMetadata(metadata.BaseFileRevisionMetadata):

    def __init__(self, raw, science_meta):
        super().__init__(raw)
        self.science_metadata = science_meta

    def _get_element(self, name):
        """Helper function for retrieving metadata fields from source document by name. Returns
        first element found.

        :param str name: Element tag.
        :rtype: str
        :return: String contents of element or None
        """
        el = self.science_metadata.getElementsByTagName(name)
        if len(el) > 0:
            return el[0].firstChild.wholeText
        else:
            return None

    @property
    def version(self):
        return 'latest'

    @property
    def version_identifier(self):
        return 'version'

    @property
    def modified(self):
        return self._get_element('dcterms:dateSubmitted')
