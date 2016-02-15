from waterbutler.core import metadata

import xml.dom.minidom


class BaseDryadMetadata(metadata.BaseMetadata):
    """ Translation from Dryad Metadata format to Waterbutler Metadata
    """

    def __init__(self, raw, doi):
        """ Constructor

        :param data_obj: The metadata source object.
        :type data_obj: DryadData o.
        :param doi: Dryad DOI. Format: doi:10.5061/dryad.XXXX
        :type doi: str.
        """
        super().__init__(xml.dom.minidom.parseString(raw))
        self.dryad_doi = doi

    def _get_element(self, name):
        el = self.raw.getElementsByTagName(name)
        if len(el) > 0:
            return el[0].firstChild.wholeText
        else:
            return ''

    def _get_element_list(self, name):
        return [i.firstChild.wholeText for i in self.raw.getElementsByTagName(name)]

    @property
    def name(self):
        return self._get_element("dcterms:title")

    @property
    def content_type(self):
        return self._get_element("dcterms:type")

    @property
    def modified(self):
        return self._get_element("dcterms:dateSubmitted")

    @property
    def provider(self):
        return 'dryad'

    @property
    def path(self):
        return "/" + self.dryad_doi.split('dryad.')[1]

    @property
    def extra(self):
        return {'doi': self.dryad_doi,
                'temporal': self._get_element("dcterms:temporal"),
                'spatial': self._get_element("dcterms:spatial"),
                'available': self._get_element("dcterms:available"),
                'scientificName': self._get_element_list("dwc:scientificName"),
                'subject': self._get_element_list("dcterms:subject"),
                'description': self._get_element("dcterms:description"),
                'rights': self._get_element("dcterms:rights"),
                'id': self._get_element("dcterms:identifier"),
                'creators': self._get_element_list("dcterms:creator")}

    @property
    def etag(self):
        return '{}::{}'.format(self.name, self.dryad_doi)

    @property
    def size(self):
        return 0

    def _json_api_links(self, resource):
        return {}


class DryadFileMetadata(BaseDryadMetadata, metadata.BaseFileMetadata):
    """
        DryadFileMetadata needs to be instantiated with the same metadata from
        the raw API call that normal packages do, AND with metadata from the file
        metadata API and information that is only found in the bit stream.
    """
    def __init__(self, raw, doi, raw_file, file_name):
        BaseDryadMetadata.__init__(self, raw, doi)
        self.raw_file = xml.dom.minidom.parseString(raw_file)
        self.file_name = file_name

    def _get_file_element(self, name):
        el = self.raw_file.getElementsByTagName(name)
        if len(el) > 0:
            return el[0].firstChild.wholeText
        else:
            return ''

    def _get_file_element_list(self, name):
        return [i.firstChild.wholeText for i in self.raw_file.getElementsByTagName(name)]

    @property
    def content_type(self):
        return self._get_file_element('formatId')

    @property
    def name(self):
        return self.file_name.strip('\"')

    @property
    def size(self):
        return self._get_file_element('size')

    @property
    def extra(self):
        return super(BaseDryadMetadata, self).extra.update({
            'part_of': self._get_element("dcterms:isPartOf")
        })


class DryadPackageMetadata(BaseDryadMetadata, metadata.BaseFolderMetadata):

    @property
    def path(self):
        return BaseDryadMetadata.path(self) + '/'

    @property
    def file_parts(self):
        return self._get_element_list("dcterms:hasPart")

    @property
    def extra(self):
        return super(BaseDryadMetadata, self).extra.update({
            'references': self._get_element("dcterms:references"),
            'file_parts': self.file_parts
        })
