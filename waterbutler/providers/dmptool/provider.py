from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.tasks.core import backgroundify

from .metadata import DmptoolFileMetadata
from .client import _connect
import datetime


def timestamp_iso(dt):

    return datetime.datetime.strptime(dt, "%m/%d/%Y").isoformat()


# @backgroundify
# def _dmptool_plans(token, host):

#     client = _connect(host, token)

#     plans = client.plans_owned()

#     # TO DO: try to compute actual length
#     results = [{'title': plan['name'],
#               'guid': str(plan['id']),
#               'created': timestamp_iso(plan['created']),
#               'updated': timestamp_iso(plan['modified']),
#               'length': 0}
#               for plan in plans]

#     print('_dmptool_plans: results: ', results)

#     return results


@backgroundify
def _dmptool_plan(plan_id, token, host):

    client = _connect(host, token)
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

    client = _connect(host, token)
    return client.plans_full(plan_id, 'pdf')


class DmptoolProvider(provider.BaseProvider):

    NAME = 'dmptool'

    def __init__(self, auth, credentials, dmptool_settings):

        super().__init__(auth, credentials, dmptool_settings)

        protocol = 'https'
        # self.api_token = self.credentials['api_token']
        # self.host = self.credentials['host']
        self.base_url = '{}://{}/api/v1/'.format(protocol, self.credentials['host'])
        self.headers = {'Authorization': 'Token token={}'.format(self.credentials['api_token'])}

    async def _package_metadata(self):
        """ Interface to file and package metadata from Dmptool

        :param path: Path mapping to waterbutler interpretation of Dmptool package
        :type path: `waterbutler.core.path.WaterButlerPath`
        :returns:  `list` -- A list of metadata
        :raises: `urllib.error.HTTPError`

        """

        # api_token = self.credentials['api_token']
        # host = self.credentials['host']

        # plans = await _dmptool_plans(api_token, host)
        plans = await self._dmptool_plans()

        return [DmptoolFileMetadata(plan) for plan in plans]

    async def _file_metadata(self, path):

        # print("_file_metadata -> path: ", path)

        api_token = self.credentials['api_token']
        host = self.credentials['host']

        plan_md = await _dmptool_plan(path, api_token, host)
        #plan_md = await self._dmptool_plan(path)

        return DmptoolFileMetadata(plan_md)

    async def metadata(self, path, **kwargs):

        # TO DO: IMPORTANT
        """
        responds to a GET request for the url you posted.
        If the `path` query arg refers to a particular plan, it should return the metadata for that plan.
        If `path` is just `/`, it should return a list of metadata objects for all plans in the root directory.
         IIRC, Dmptool doesn’t have a hierarchy, so the root directory is just a collection of all available plans.
        """

        if str(path) == u'/':
            package = await self._package_metadata()
            return package

        if not path.is_dir:
            # print("metdata path.path:", path.path)
            return (await self._file_metadata(path.path))

    async def download(self, path, **kwargs):
        """ Interface to downloading files from Dmptool

        :param path: Path mapping to waterbutler interpretation of Dmptool file
        :type path: `waterbutler.core.path.WaterButlerPath`
        :returns:  `waterbutler.core.streams.ResponseStreamReader` Download stream generator
        :raises:   `waterbutler.core.exceptions.DownloadError`
        """

        try:
            api_token = self.credentials['api_token']
            host = self.credentials['host']
            plan_id = path.parts[1].raw
            pdf = await _dmptool_plan_pdf(plan_id, api_token, host)
            # pdf = await self._dmptool_plan_pdf(plan_id)
        except Exception as e:
            # TO DO: throw the correct exception
            print("DmptoolProvider.download: exception", e)
        else:
            print("DmptoolProvider.download: api_token, host, type(pdf)", path, api_token, host, type(pdf))

        stream = streams.StringStream(pdf)
        stream.content_type = 'application/pdf'
        stream.name = '{}.pdf'.format(plan_id)

        # # modeling after gdoc provider
        # # https://github.com/CenterForOpenScience/waterbutler/blob/develop/waterbutler/providers/googledrive/provider.py#L181-L185

        return stream

    async def validate_path(self, path, **kwargs):
        """
        :param path: Path to either a package or file.
        :type path: `str`
        """

        print("DmptoolProvider.validate_path: path", path)
        wbpath = WaterButlerPath(path)

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

        wbpath = await self.validate_path(path, **kwargs)

        if wbpath.is_root:
            return wbpath

        api_token = self.credentials['api_token']
        host = self.credentials['host']

        # plan = await _dmptool_plan(wbpath.parts[1].raw, api_token, host)
        plan = await self._dmptool_plan(wbpath.parts[1].raw)

        if isinstance(plan, Exception):
            raise exceptions.NotFoundError(str(path))
        else:
            # TO: actually validate the plan
            # for now just return wbpath
            if True:
                return wbpath
            else:
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

    def get_url(self, path, headers=None):
        if headers is None:
            headers = self.headers

        url = self.base_url + path
        response = requests.get(url, headers=headers)

        response.raise_for_status()
        return response

    async def get_url_async(self, path, headers=None):

        # https://github.com/CenterForOpenScience/waterbutler/blob/305c0aa57bd0066b8f0a4186b0fbb2067a97cafc/waterbutler/core/provider.py#L140-L176

        if headers is None:
            headers = self.headers

        url = self.base_url + path

        resp = await self.make_request('GET', url,
            headers=headers,
            expects=(200,),
            throws=exceptions.MetadataError
        )

        return resp

    def _unroll(self, plans):
        """
        each plan is a dict with a key plan
        """
        return [
            plan.get('plan')
            for plan in plans
        ]

    async def plans(self, id_=None):
        """
        https://dmptool.org/api/v1/plans
        https://dmptool.org/api/v1/plans/:id
        """
        print ("DmptoolProvider.plans: params ", id_)

        if id_ is None:
            resp = await self.get_url_async('plans')
            r = await resp.json()
            result = self._unroll(r)
        else:
            resp = await self.get_url_async('plans/{}'.format(id_))
            r = await resp.json()
            result = r.get('plan')

        print ("DmptoolProvider.plans: result ", result)
        return result

    async def plans_full(self, id_=None, format_='json'):

        if id_ is None:
            # a json doc for to represent all public docs
            # I **think** if we include token, will get only docs owned
            resp = await self.get_url_async('plans_full/', headers={})
            r = await resp.json()
            return self._unroll(r)
        else:
            if format_ == 'json':
                resp = await self.get_url_async('plans_full/{}'.format(id_))
                r = await resp.json()
                return r.get('plan')
            elif format_ in ['pdf', 'docx']:
                resp = await self.get_url_async('plans_full/{}.{}'.format(id_, format_))
                r = await resp.read()
                return r
            else:
                return None

    async def plans_owned(self):
        resp = await self.get_url_async('plans_owned')
        r = await resp.json()
        return self._unroll(r)

    def plans_owned_full(self):
        return self._unroll(self.get_url('plans_owned_full').json())

    def plans_templates(self):
        return self._unroll(self.get_url('plans_templates').json())

    def institutions_plans_count(self):
        """
        https://github.com/CDLUC3/dmptool/wiki/API#for-a-list-of-institutions-and-plans-count
        """
        plans_counts = self.get_url('institutions_plans_count').json()
        return plans_counts

    async def _dmptool_plans(self):

        plans = await self.plans_owned()

        # TO DO: try to compute actual length
        results = [{'title': plan['name'],
                  'guid': str(plan['id']),
                  'created': timestamp_iso(plan['created']),
                  'updated': timestamp_iso(plan['modified']),
                  'length': 0}
                  for plan in plans]

        print('provider._dmptool_plans: results: ', results)

        return results

    async def _dmptool_plan(self, plan_id):

        try:
            plan = await self.plans(id_=plan_id)
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

    async def _dmptool_plan_pdf(self, plan_id):

        return await self.plans_full(plan_id, 'pdf')
