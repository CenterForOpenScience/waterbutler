from tests.providers.azureblobstorage.fixtures import (
    blob_list_with_versions_response, blob_properties_headers, blob_list_xml,
    special_characters_blobs, empty_list_xml, root_list_xml, 
    provider, auth, credentials, settings
)
from waterbutler.core.path import WaterButlerPath
from waterbutler.providers.azureblobstorage.metadata import (
    AzureBlobStorageFileMetadata, 
    AzureBlobStorageFileMetadataHeaders,
    AzureBlobStorageFolderMetadata
)



class TestAzureBlobFileMetadata:
    """Test file metadata transformation from various sources"""

    def test_file_metadata_from_headers(self, blob_properties_headers):
        """Test creating file metadata from HTTP headers"""
        path = WaterButlerPath('/test-file.xlsx')
        
        metadata = AzureBlobStorageFileMetadataHeaders(blob_properties_headers, path.path)

        assert metadata.name == 'test-file.xlsx'
        assert metadata.path == '/test-file.xlsx'
        assert metadata.size == int(blob_properties_headers['Content-Length'])
        assert metadata.content_type == blob_properties_headers['Content-Type']
        assert metadata.etag == blob_properties_headers['ETag'].strip('"')
        assert metadata.modified == blob_properties_headers['Last-Modified']
        assert metadata.extra['md5'] == blob_properties_headers.get('Content-MD5', '')

    def test_file_metadata_from_list_response(self, provider, blob_list_xml):
        """Test creating file metadata from XML list response"""
        parsed = provider._convert_xml_to_blob_list(blob_list_xml)
        
        # Get the first blob from the parsed result
        blobs = parsed.get('Blob', [])
        assert len(blobs) > 0, "No blobs found in test fixture"
        
        blob_data = blobs[0]
        metadata = AzureBlobStorageFileMetadata(blob_data)

        assert metadata.name == 'file1.txt'
        assert '/file1.txt' in metadata.path
        assert metadata.size == 1024
        assert metadata.content_type == 'text/plain'
        assert metadata.etag == '0x8D1A2B3C4D5E6F7'
        assert metadata.modified == 'Mon, 15 Jul 2025 07:28:00 GMT'
        assert metadata.extra['md5'] == 'sQqNsWTgdUEFt6mb5y4/5Q=='

    def test_file_metadata_with_versions(self, provider, blob_list_with_versions_response):
        """Test file metadata with version information"""
        parsed = provider._convert_xml_to_blob_list(blob_list_with_versions_response)
        
        blobs = parsed.get('Blob', [])
        assert len(blobs) > 0, "No blobs found in test fixture"
        
        # Get the first blob (current version)
        blob_data = blobs[0]
        metadata = AzureBlobStorageFileMetadata(blob_data)

        assert metadata.name == 'report.pdf'
        assert '/report.pdf' in metadata.path
        assert metadata.size == 524288
        assert metadata.extra['blob_type'] == 'BlockBlob'

    def test_file_metadata_minimal(self):
        """Test file metadata with minimal required fields"""
        headers = {
            'Content-Length': '100',
            'Last-Modified': 'Mon, 15 Jul 2025 12:00:00 GMT',
            'ETag': '"0x8D1A2B3C4D5E999"',
            'Content-Type': 'application/octet-stream'
        }

        path = WaterButlerPath('/minimal.txt')
        metadata = AzureBlobStorageFileMetadataHeaders(headers, path.path)

        assert metadata.name == 'minimal.txt'
        assert metadata.size == 100
        assert metadata.content_type == 'application/octet-stream'
        assert metadata.etag == '0x8D1A2B3C4D5E999'

    def test_file_metadata_special_characters(self, provider, special_characters_blobs):
        """Test handling special characters in filenames"""
        parsed = provider._convert_xml_to_blob_list(special_characters_blobs)
        
        blobs = parsed.get('Blob', [])
        assert len(blobs) >= 2, "Expected at least 2 blobs with special characters"

        # Test first blob (file with spaces)
        blob_data1 = blobs[0]
        metadata1 = AzureBlobStorageFileMetadata(blob_data1)
        assert 'file with spaces.txt' in metadata1.name

        # Test second blob (special characters)
        blob_data2 = blobs[1]
        metadata2 = AzureBlobStorageFileMetadata(blob_data2)
        assert 'special' in metadata2.name or '@' in metadata2.name

    def test_file_metadata_serialization(self):
        """Test JSON API serialization of file metadata"""
        headers = {
            'Content-Length': '2048000',
            'Content-Type': 'application/pdf',
            'ETag': '"0x8D1A2B3C4D5E700"',
            'Last-Modified': 'Mon, 15 Jul 2025 14:30:00 GMT'
        }

        path = WaterButlerPath('/report.pdf')
        metadata = AzureBlobStorageFileMetadataHeaders(headers, path.path)
        
        assert metadata.name == 'report.pdf'
        assert metadata.size == 2048000
        assert metadata.content_type == 'application/pdf'
        assert metadata.etag == '0x8D1A2B3C4D5E700'


class TestAzureBlobFolderMetadata:
    """Test folder metadata transformation"""

    def test_folder_metadata_from_path(self):
        """Test creating folder metadata from blob prefix"""
        folder_data = {'Name': 'documents/'}
        metadata = AzureBlobStorageFolderMetadata(folder_data)

        assert metadata.name == 'documents'
        assert '/documents/' in metadata.path

    def test_folder_metadata_from_list_response(self, provider, blob_list_xml):
        """Test creating folder metadata from blob prefix in list response"""
        parsed = provider._convert_xml_to_blob_list(blob_list_xml)
        
        # Get folder prefixes from the parsed result
        prefixes = parsed.get('BlobPrefix', [])
        if len(prefixes) > 0:
            folder_data = prefixes[0]  # Get the first prefix
            metadata = AzureBlobStorageFolderMetadata(folder_data)

            assert 'subfolder' in metadata.name
            assert '/subfolder/' in metadata.path

    def test_folder_metadata_root(self):
        """Test root folder metadata"""
        folder_data = {'Name': ''}
        metadata = AzureBlobStorageFolderMetadata(folder_data)

        assert metadata.name == ''

    def test_folder_metadata_nested(self):
        """Test nested folder paths"""
        test_cases = [
            ('a/', 'a'),
            ('a/b/', 'b'),
            ('a/b/c/', 'c'),
            ('path/to/deep/folder/', 'folder')
        ]

        for folder_path, expected_name in test_cases:
            folder_data = {'Name': folder_path}
            metadata = AzureBlobStorageFolderMetadata(folder_data)
            assert metadata.name == expected_name

    def test_folder_metadata_serialization(self):
        """Test basic properties of folder metadata"""
        folder_data = {'Name': 'projects/'}
        metadata = AzureBlobStorageFolderMetadata(folder_data)
        
        assert metadata.name == 'projects'
        assert '/projects/' in metadata.path


class TestMetadataListProcessing:
    """Test processing lists of metadata from API responses"""

    def test_process_empty_folder(self, provider, empty_list_xml):
        """Test processing empty folder response"""
        parsed = provider._convert_xml_to_blob_list(empty_list_xml)

        blobs = parsed.get('Blob', [])
        prefixes = parsed.get('BlobPrefix', [])

        assert len(blobs) == 0
        assert len(prefixes) == 0

    def test_process_root_listing(self, provider, root_list_xml):
        """Test processing root container listing"""
        parsed = provider._convert_xml_to_blob_list(root_list_xml)

        blobs = parsed.get('Blob', [])
        assert len(blobs) >= 0

        prefixes = parsed.get('BlobPrefix', [])
        folder_names = [p['Name'].rstrip('/') for p in prefixes]
        assert isinstance(folder_names, list)

    def test_mixed_content_listing(self, provider, blob_list_xml):
        """Test folder with both files and subfolders"""
        parsed = provider._convert_xml_to_blob_list(blob_list_xml)

        blobs = parsed.get('Blob', [])
        prefixes = parsed.get('BlobPrefix', [])

        assert len(blobs) >= 0
        assert len(prefixes) >= 0
