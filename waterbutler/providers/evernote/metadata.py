from waterbutler.core import metadata

import xml.dom.minidom


class BaseEvernoteMetadata(metadata.BaseMetadata):
    """ Translation from Evernote Metadata format to Waterbutler Metadata
    """

    def __init__(self, raw, doi):
        """
        :param raw: Source metadata generated from Evernote API. Must be parseable by xml.dom.minidom
        :type raw: str
        :param doi: Evernote DOI. Format: doi:10.5061/evernote.XXXX
        :type doi: str.
        """
        super().__init__(xml.dom.minidom.parseString(raw))
        self.evernote_doi = doi

    def _get_element_(self, name):
        """
            Helper function for retrieving metadata fields from source document
            by name. Returns first element found

            :param name: Element tag.
            :type name: string or unicode
            :returns String contents of element
        """
        el = self.raw.getElementsByTagName(name)
        if len(el) > 0:
            return el[0].firstChild.wholeText
        else:
            return ''

    def _get_element_list_(self, name):
        """
            Helper function for retrieving metadata fields from source document
            by name.

            :param name: Element tag.
            :type name: string or unicode
            :returns String contents of element
        """
        return [i.firstChild.wholeText for i in self.raw.getElementsByTagName(name)]

    @property
    def name(self):
        return self._get_element_("dcterms:title")

    @property
    def content_type(self):
        return self._get_element_("dcterms:type")

    @property
    def modified(self):
        return self._get_element_("dcterms:dateSubmitted")

    @property
    def provider(self):
        return 'evernote'

    @property
    def path(self):
        return "/" + self.evernote_doi.split('evernote.')[1]

    @property
    def extra(self):
        return {'doi': self.evernote_doi,
                'temporal': self._get_element_("dcterms:temporal"),
                'spatial': self._get_element_("dcterms:spatial"),
                'available': self._get_element_("dcterms:available"),
                'scientificName': self._get_element_list_("dwc:scientificName"),
                'subject': self._get_element_list_("dcterms:subject"),
                'description': self._get_element_("dcterms:description"),
                'rights': self._get_element_("dcterms:rights"),
                'id': self._get_element_("dcterms:identifier"),
                'creators': self._get_element_list_("dcterms:creator")}

    @property
    def etag(self):
        return '{}::{}'.format(self.name, self.evernote_doi)

    @property
    def size(self):
        return 0

    def _json_api_links(self, resource):
        return {}


class EvernoteFileMetadata(BaseEvernoteMetadata, metadata.BaseFileMetadata):
    """
        EvernoteFileMetadata needs to be instantiated with the same metadata from
        the raw API call that normal packages do, AND with metadata from the file
        metadata API and information that is only found in the bit stream.
    """
    def __init__(self, raw, doi, raw_file, file_name):
        BaseEvernoteMetadata.__init__(self, raw, "doi:10.5061/evernote." + doi)
        self.raw_file = xml.dom.minidom.parseString(raw_file)
        self.file_name = file_name

    def _get_file_element_(self, name):
        el = self.raw_file.getElementsByTagName(name)
        if len(el) > 0:
            return el[0].firstChild.wholeText
        else:
            return ''

    def _get_file_element_list_(self, name):
        return [i.firstChild.wholeText for i in self.raw_file.getElementsByTagName(name)]

    @property
    def content_type(self):
        return self._get_file_element_('formatId')

    @property
    def name(self):
        return self.file_name.strip('\"')

    @property
    def size(self):
        return self._get_file_element_('size')

    @property
    def extra(self):
        return super(BaseEvernoteMetadata, self).extra.update({
            'part_of': self._get_element_("dcterms:isPartOf")
        })


class EvernotePackageMetadata(BaseEvernoteMetadata, metadata.BaseFolderMetadata):

    @property
    def path(self):
        return super().path + '/'

    @property
    def file_parts(self):
        return self._get_element_list_("dcterms:hasPart")

    @property
    def extra(self):
        return super(BaseEvernoteMetadata, self).extra.update({
            'references': self._get_element_("dcterms:references"),
            'file_parts': self.file_parts
        })
