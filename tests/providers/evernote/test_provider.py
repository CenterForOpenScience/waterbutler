import pytest
from http import client
from urllib import parse

from unittest import mock

from waterbutler.core import exceptions
from waterbutler.tasks.core import backgroundify
from waterbutler.core.path import WaterButlerPath
import waterbutler.providers.evernote.provider as evernote_provider
from waterbutler.providers.evernote.provider import (EvernoteProvider, EvernotePath)
from waterbutler.providers.evernote.metadata import EvernoteFileMetadata

# I need to come back to this when I merge the latest code for WB
# from tests.providers.osfstorage.fixtures import provider as osfstorage_provider

from tests.providers.osfstorage.test_provider import provider as osfstorage_provider
from tests.providers.evernote.fixtures import (auth,
  credentials, other_credentials, settings, provider, other_provider,
  mock_evernote_note, mock_evernote_notes, MockNoteStore, mock_evernote_note_store,
  mock_evernote)


class TestValidatePath:

    @pytest.mark.asyncio
    async def test_validate_v1_path_file(self, provider, mock_evernote):

      note_id = '71ce96e5-463b-4a72-9fc7-8cdcde7862d4'

      try:
          wb_path_v1 = await provider.validate_v1_path('/' + note_id)
      except Exception as exc:
          pytest.fail(str(exc))
 
      with pytest.raises(exceptions.NotFoundError) as exc:
          await provider.validate_v1_path('/' + note_id + '/')

      assert exc.value.code == client.NOT_FOUND

      wb_path_v0 = await provider.validate_path('/' + note_id)

      assert wb_path_v1 == wb_path_v0


    @pytest.mark.asyncio
    async def test_validate_path_root(self, provider, mock_evernote):

        path = await provider.validate_path('/')
        assert path.is_dir
        assert len(path.parts) == 1
        assert path.name == ''

    @pytest.mark.asyncio
    async def test_validate_v1_path_root(self, provider, mock_evernote):

        path = await provider.validate_v1_path('/')
        assert path.is_dir
        assert len(path.parts) == 1
        assert path.name == ''

    @pytest.mark.asyncio
    async def test_validate_v1_path_bad_path(self, provider, mock_evernote):

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.validate_v1_path('/bulbasaur')

        print(e.value.message, e.value.code)
        assert e.value.message == 'Could not retrieve file or directory /bulbasaur'
        assert e.value.code == 404

    @pytest.mark.asyncio
    async def test_validate_path_bad_path(self, provider, mock_evernote):

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.validate_path('/bulbasaur/charmander')

        print(e.value.message, e.value.code)
        assert e.value.message == 'Could not retrieve file or directory /bulbasaur/charmander'
        assert e.value.code == 404


class TestDownload:

    @pytest.mark.asyncio
    async def test_download(self, provider, mock_evernote):

        note_guid = '71ce96e5-463b-4a72-9fc7-8cdcde7862d4'
        note_name = 'another note for OSF Notebook'

        wbpath = EvernotePath("/" + parse.quote(note_name, safe=''), _ids=('/', note_guid))

        try:
          result = await provider.download(wbpath)
          content = await result.read()
        except Exception as e:
          print (e)
          assert False
        else:
          print(content)
          assert content == b'<div>\n hi there\n <div>\n </div>\n</div>'
        assert True

    # I don't have download(id, revision) signature
    # @pytest.mark.asyncio
    # async def test_download_revision(self, provider, mock_evernote):

    #     note_guid = '71ce96e5-463b-4a72-9fc7-8cdcde7862d4'
    #     note_name = 'another note for OSF Notebook'
    #     revision = 'latest'

    #     wbpath = EvernotePath("/" + parse.quote(note_name, safe=''), _ids=('/', note_guid))

    #     result = await provider.download(wbpath, revision)
    #     content = await result.read()

    #     print(content)

    @pytest.mark.asyncio
    async def test_download_not_found(self, provider, mock_evernote):
        note_name = 'nonexistent note'
        note_guid = 'xxxxxxxxxx'

        wbpath = EvernotePath("/" + parse.quote(note_name, safe=''), _ids=('/', note_guid))

        with pytest.raises(exceptions.DownloadError) as e:
            await provider.download(wbpath)

        assert e.value.code == 500

class TestMetadata:

    @pytest.mark.asyncio
    async def test_must_not_be_none(self, provider, mock_evernote):
        note_name = 'nonexistent note'
        note_guid = 'xxxxxxxxxx'

        wbpath = EvernotePath("/" + parse.quote(note_name, safe=''), _ids=('/', note_guid))

        with pytest.raises(Exception) as e:
            await provider.metadata(wbpath)

        assert e.value.code == 400
        assert e.value.message == 'Note note found'


    @pytest.mark.asyncio
    async def test_download_not_found(self, provider, mock_evernote):

        note_name = 'nonexistent note'
        note_guid = 'xxxxxxxxxx'

        wbpath = EvernotePath("/" + parse.quote(note_name, safe=''), _ids=('/', note_guid))

        # note_md = await _evernote_note(wbpath, token, withContent=False)

        with pytest.raises(exceptions.DownloadError) as e:
            await provider.download(wbpath)

    @pytest.mark.asyncio
    async def test_folder_metadata(self, provider, credentials, mock_evernote):

        notebook_guid = '14d9a008-f467-443a-9fda-a9ea91960c8b'
        notebook_name = 'OSF Notebook'

        token = credentials['token']
        notes = await evernote_provider._evernote_notes(notebook_guid, token)

        notes_md = [EvernoteFileMetadata(note) for note in notes]

        assert len(notes_md) == 3

    @pytest.mark.asyncio
    async def test_root_metadata(self, provider, credentials, mock_evernote):

        wbpath = EvernotePath("/" , _ids=('/',))
        root_md = await provider.metadata(wbpath)

        notebook_guid = '14d9a008-f467-443a-9fda-a9ea91960c8b'
        notebook_name = 'OSF Notebook'

        token = credentials['token']
        notes = await evernote_provider._evernote_notes(notebook_guid, token)

        notes_md = [EvernoteFileMetadata(note) for note in notes]

        assert root_md == notes_md


    @pytest.mark.asyncio
    async def test_metadata(self, provider, credentials, mock_evernote):

        note_guid = '71ce96e5-463b-4a72-9fc7-8cdcde7862d4'
        note_name = 'another note for OSF Notebook'

        wbpath = EvernotePath("/" + parse.quote(note_name, safe=''), _ids=('/', note_guid))
        result = await provider.metadata(wbpath)

        token = credentials['token']

        note = await evernote_provider._evernote_note(note_guid, token, withContent=False)
        note_md = EvernoteFileMetadata(note)

        assert result == note_md

    @pytest.mark.asyncio
    async def test_metadata_bad_response(self, provider, mock_evernote):

        note_name = 'nonexistent note'
        note_guid = 'xxxxxxxxxx'

        wbpath = EvernotePath("/" + parse.quote(note_name, safe=''), _ids=('/', note_guid))

        with pytest.raises(Exception) as e:
            await provider.metadata(wbpath)

        assert e.value.code == 400
        assert e.value.message == 'Note note found'


class TestRevisions:

    @pytest.mark.asyncio
    async def test_get_revisions(self, provider, mock_evernote):

        note_guid = '71ce96e5-463b-4a72-9fc7-8cdcde7862d4'
        note_name = 'another note for OSF Notebook'

        wbpath = EvernotePath("/" + parse.quote(note_name, safe=''), _ids=('/', note_guid))

        revisions = await provider.revisions(wbpath)
        assert len(revisions) == 1


class TestOperations:

    @pytest.mark.asyncio
    async def test_can_duplicate_names(self, provider, mock_evernote):
        assert provider.can_duplicate_names() is False

    @pytest.mark.asyncio
    async def test_can_intra_move(self, provider, mock_evernote):
        assert provider.can_intra_move(provider) is False

    @pytest.mark.asyncio
    async def test_can_intra_copy(self, provider, mock_evernote):
        assert provider.can_intra_copy(provider) is False

    @pytest.mark.asyncio
    async def test_intra_copy(self, provider, other_provider, mock_evernote):
        src_path =  EvernotePath("/a", _ids=('/', '1'))
        dest_path = EvernotePath("/b", _ids=('/', '2'))

        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
            result = await provider.intra_copy(other_provider, src_path, dest_path)

    @pytest.mark.asyncio
    async def test_do_intra_move_or_copy(self, provider, other_provider, mock_evernote):
        src_path =  EvernotePath("/a", _ids=('/', '1'))
        dest_path = EvernotePath("/b", _ids=('/', '2'))

        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
            result = await provider._do_intra_move_or_copy(other_provider, src_path, dest_path)

    @pytest.mark.asyncio
    async def test_copy(self, provider, mock_evernote):
#   async def test_copy(self, provider, osfstorage_provider, mock_evernote)

        # let's test trying to copy from Evernote to Evernote
        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
          result = await provider.copy(provider)

        # TO DO: if other_provider were an OSF Storage -- maybe I'll need to mock
        # Error for now -- catch it and fix later
        # try:
        #   result = await provider.copy(osfstorage_provider)
        # except Exception as e:
        #   assert True

    @pytest.mark.asyncio
    async def test_move(self, provider, mock_evernote):
        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
          result = await provider.move()

    @pytest.mark.asyncio
    async def test_delete(self, provider, mock_evernote):
        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
          result = await provider.delete()

    @pytest.mark.asyncio
    async def test_upload(self, provider, mock_evernote):
        with pytest.raises(exceptions.ReadOnlyProviderError) as e:
          result = await provider.upload(None)


class TestMisc:

    @pytest.mark.asyncio
    async def test_can_duplicate_name(self, provider, mock_evernote):
        assert provider.can_duplicate_names() == False

    @pytest.mark.asyncio
    async def test_path_from_metadata(self, provider, credentials, mock_evernote):

        note_guid = '71ce96e5-463b-4a72-9fc7-8cdcde7862d4'
        note_name = 'another note for OSF Notebook'
        token = credentials['token']

        wbpath = EvernotePath("/" + parse.quote(note_name, safe='') + '.html', _ids=('/', note_guid))

        note = await evernote_provider._evernote_note(note_guid, token, withContent=False)
        note_md = EvernoteFileMetadata(note)

        child_path =  provider.path_from_metadata(wbpath.parent, note_md)

        assert child_path.full_path == wbpath.full_path
        assert child_path == wbpath