from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.tasks.core import backgroundify

from .metadata import DmptoolFileMetadata
from .client import DMPTool
import datetime


def timestamp_iso(dt):

    return datetime.datetime.strptime(dt, "%m/%d/%Y").isoformat()


@backgroundify
def _dmptool_plans(token, host):

    client = DMPTool(token, host)

    plans = client.plans_owned()
    print('_dmptool_plans: token, plans: ', token, plans)

    # TO DO:  length
    results = [{'title': plan['name'],
              'guid': str(plan['id']),
              'created': timestamp_iso(plan['created']),
              'updated': timestamp_iso(plan['modified']),
              'length': 0}
              for plan in plans]

    print('_dmptool_plans: results: ', results)

    return results


@backgroundify
def _dmptool_plan(plan_id, token, host):

    client = DMPTool(token, host)
    try:
        plan = client.plans(id_=plan_id)
    except Exception as e:
        return e
    else:
        result = {'title': plan['name'],
              'guid': str(plan['id']),
              'created': timestamp_iso(plan['created']),
              'updated': timestamp_iso(plan['modified']),
              'length': 0,
              'content': ''}
        return result


@backgroundify
def _dmptool_plan_pdf(plan_id, token, host):
    client = DMPTool(token, host)
    return client.plans_full(plan_id, 'pdf')


class DmptoolProvider(provider.BaseProvider):

    NAME = 'dmptool'

    def __init__(self, auth, credentials, dmptool_settings):

        print('DmptoolProvider.__init__:auth, credentials, dmptool_settings', auth, credentials, dmptool_settings)
        super().__init__(auth, credentials, dmptool_settings)

    async def _package_metadata(self):
        """ Interface to file and package metadata from Dmptool

        :param path: Path mapping to waterbutler interpretation of Dmptool package
        :type path: `waterbutler.core.path.WaterButlerPath`
        :returns:  `list` -- A list of metadata
        :raises: `urllib.error.HTTPError`

        """

        api_token = self.credentials['api_token']
        host = self.credentials['host']

        # notebook_guid = self.settings['folder']
        print('DmptoolProvider._package_metadata:credentials', self.credentials)
        print('DmptoolProvider._package_metadata:settings', self.settings)
        print('DmptoolProvider._package_metadata:api_token', api_token)
        plans = await _dmptool_plans(api_token, host)

        return [DmptoolFileMetadata(plan) for plan in plans]

    async def _file_metadata(self, path):

        print("_file_metadata -> path: ", path)

        api_token = self.credentials['api_token']
        host = self.credentials['host']

        plan_md = await _dmptool_plan(path, api_token, host)

        return DmptoolFileMetadata(plan_md)

    async def metadata(self, path, **kwargs):

        # TO DO: IMPORTANT
        """
        responds to a GET request for the url you posted.
        If the `path` query arg refers to a particular plan, it should return the metadata for that plan.
        If `path` is just `/`, it should return a list of metadata objects for all plans in the root directory.
         IIRC, Dmptool doesn’t have a hierarchy, so the root directory is just a collection of all available plans.
        """

        print("metadata: path: {}".format(path), type(path), path.is_dir)

        if str(path) == u'/':
            package = await self._package_metadata()
            return package

        if not path.is_dir:
            print("metdata path.path:", path.path)
            return (await self._file_metadata(path.path))

    async def download(self, path, **kwargs):
        """ Interface to downloading files from Dmptool

        :param path: Path mapping to waterbutler interpretation of Dmptool file
        :type path: `waterbutler.core.path.WaterButlerPath`
        :returns:  `waterbutler.core.streams.ResponseStreamReader` Download stream generator
        :raises:   `waterbutler.core.exceptions.DownloadError`
        """

        # ResponseStreamReader:
        # https://github.com/CenterForOpenScience/waterbutler/blob/63b7d469e5545de9f2183b964fb6264fd9a423a5/waterbutler/core/streams/http.py#L141-L183

        # print("dmptool provider download: starting")
        # token = self.credentials['token']
        # client = get_dmptool_client(token)

        # note_guid = path.parts[1].raw
        # note_metadata = await _dmptool_plan(note_guid, token, withContent=True)

        # # convert to HTML
        # mediaStore = OSFMediaStore(client.get_note_store(), note_guid)
        # html = ENML2HTML.ENMLToHTML(note_metadata["content"], pretty=True, header=False,
        #       media_store=mediaStore)

        try:
            api_token = self.credentials['api_token']
            host = self.credentials['host']
            plan_id = path.parts[1].raw
            pdf = await _dmptool_plan_pdf(plan_id, api_token, host)
        except Exception as e:
            print("DmptoolProvider.download: exception", e)
        else:
            print("DmptoolProvider.download: api_token, host, type(pdf)", path, api_token, host, type(pdf))

        # how to actually read the pdf file....

        # HACK -- let me read a test pdf and stream
        # test_pdf = open('/Users/raymondyee/Downloads/21222.pdf', 'rb').read()

        stream = streams.StringStream(pdf)
        stream.content_type = 'application/pdf'
        stream.name = '{}.pdf'.format(plan_id)

        # # modeling after gdoc provider
        # # https://github.com/CenterForOpenScience/waterbutler/blob/develop/waterbutler/providers/googledrive/provider.py#L181-L185

        print("dmptool provider download: finishing")
        return stream

    async def validate_path(self, path, **kwargs):
        """
        :param path: Path to either a package or file.
        :type path: `str`
        """

        print("in Dmptool.validate_path. path, type(path):", path, type(path))

        wbpath = WaterButlerPath(path)

        print("wbpath.is_root: {}".format(wbpath.is_root))
        print("wbpath.is_dir: {}".format(wbpath.is_dir))
        print("len(wbpath.parts): {}".format(len(wbpath.parts)))

        if wbpath.is_root:
            return wbpath
        if len(wbpath.parts) == 2 and wbpath.is_dir:
                raise exceptions.NotFoundError(path)
        if len(wbpath.parts) > 2:
            raise exceptions.NotFoundError(path)

        return wbpath

    async def validate_v1_path(self, path, **kwargs):
        """
            See :func:`waterbutler.providers.dmptool.provider.DmptoolProvider.validate_path`.
            Additionally queries the Dmptool API to check if the package exists.
        """

        print("in Dmptool.validate_v1_path. path: {}".format(path),
               "kwargs: ", kwargs)

        wbpath = await self.validate_path(path, **kwargs)

        if wbpath.is_root:
            return wbpath

        api_token = self.credentials['api_token']
        host = self.credentials['host']

        print("wbpath.parts[1].raw", wbpath.parts[1].raw)
        plan = await _dmptool_plan(wbpath.parts[1].raw, api_token, host)

        if isinstance(plan, Exception):
            print("validate_v1_path. could not get Plan", plan)
            raise exceptions.NotFoundError(str(path))
        else:
            # TO: actually validate the plan
            # for now just return wbpath
            if True:
                return wbpath
            else:
                print("plan does not exist")
                raise exceptions.NotFoundError(str(path))

    def can_intra_move(self, other, path=None):
        """
            Moves are not allowed. Only Copies from Dmptool to another provider.

            Raises:
                `waterbutler.core.exceptions.ReadOnlyProviderError` Always
        """
        return False

    def can_intra_copy(self, other, path=None):
        """
            All files in Dmptool are able to be copied out (if accessible).

            :returns: `True` Always
        """
        return False

    async def intra_copy(self, dest_provider, src_path, dest_path):
        """
            Accesses Dmptool file bitstream for file access.
            Package access raises 404 error.
            Must access file metadata first in order to get file name and size.

            :returns: `coroutine` File stream generated by :func:`waterbutler.providers.dmptool.provider.DmptoolProvider.download`
        """
        raise exceptions.ReadOnlyProviderError(self)

    async def _do_intra_move_or_copy(self, dest_provider, src_path, dest_path):
        """
            Accesses Dmptool file bitstream for file access.
            Package access raises 404 error.
            Must access file metadata first in order to get file name and size.

            :returns: `coroutine`
        """

        raise exceptions.ReadOnlyProviderError(self)

    async def upload(self, stream, **kwargs):
        """
        Uploads are not allowed.

        Raises:
            `waterbutler.core.exceptions.ReadOnlyProviderError` Always
        """
        raise exceptions.ReadOnlyProviderError(self)

    async def delete(self, **kwargs):
        """
        Deletions are not allowed.

        Raises:
            exceptions.ReadOnlyProviderError: Always
        """
        raise exceptions.ReadOnlyProviderError(self)

    def can_duplicate_names(self):
        """
            Dmptool write access is not allowed.

        Raises:
            `waterbutler.core.exceptions.ReadOnlyProviderError` Always
        """
        return False
