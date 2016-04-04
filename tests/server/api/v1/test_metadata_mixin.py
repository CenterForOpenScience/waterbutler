import pytest
from unittest import mock

from waterbutler.server.api.v1.provider.metadata import MetadataMixin


class BaseMetadataMixinTest:

    def setup_method(self, method):
        self.mixin = MetadataMixin()
        self.mixin.write = mock.Mock()
        self.mixin.request = mock.Mock()
        self.mixin.set_status = mock.Mock()


@pytest.mark.skipif
class TestHeaderMetadata(BaseMetadataMixinTest):

    def test_revision(self):
        pass

    def test_version(self):
        pass

    def test_size_none(self):
        pass

    def test_modified_none(self):
        pass

    def test_content_type_default(self):
        pass

    def test_x_waterbutler_metadata(self):
        pass


@pytest.mark.skipif
class TestGetFolder(BaseMetadataMixinTest):

    def test_zip(self):
        pass

    def test_listing(self):
        pass


@pytest.mark.skipif
class TestGetFile(BaseMetadataMixinTest):

    def test_meta(self):
        pass

    def test_versions(self):
        pass

    def test_revisions(self):
        pass

    def test_download_file(self):
        pass


@pytest.mark.skipif
class TestDownloadFile(BaseMetadataMixinTest):

    def test_range(self):
        pass

    def test_version(self):
        pass

    def test_revision(self):
        pass

    def test_mode(self):
        pass

    def test_direct(self):
        pass

    def test_redirect(self):
        pass

    def test_content_type(self):
        pass

    def test_content_length(self):
        pass

    def test_display_name(self):
        pass

    def test_stream_name(self):
        pass

    def test_mime_type(self):
        pass


@pytest.mark.skipif
class TestFileMetadata(BaseMetadataMixinTest):

    def test_version(self):
        pass

    def test_revision(self):
        pass

    def test_return(self):
        pass


@pytest.mark.skipif
class TestFileRevisions(BaseMetadataMixinTest):

    def test_return(self):
        pass

    def test_not_coroutine(self):
        pass


@pytest.mark.skipif
class TestDownloadFolderAsZip(BaseMetadataMixinTest):

    def test_return(self):
        pass
