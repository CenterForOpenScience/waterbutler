import pytest


@pytest.fixture
def package_scientific_metadata():
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
def file_scientific_metadata():
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
def file_system_metadata():
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
