from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from .metadata import DmptoolFileMetadata
import datetime


def timestamp_iso(dt):

    return datetime.datetime.strptime(dt, "%m/%d/%Y").isoformat()


class DmptoolProvider(provider.BaseProvider):

    NAME = 'dmptool'

    def __init__(self, auth, credentials, dmptool_settings):

        super().__init__(auth, credentials, dmptool_settings)

        protocol = 'https'
        self.base_url = '{}://{}/api/v1/'.format(protocol, self.credentials['host'])
        self.headers = {'Authorization': 'Token token={}'.format(self.credentials['api_token'])}

    async def _package_metadata(self):
        """ Interface to file and package metadata from Dmptool

        :param path: Path mapping to waterbutler interpretation of Dmptool package
        :type path: `waterbutler.core.path.WaterButlerPath`
        :returns:  `list` -- A list of metadata
        :raises: `urllib.error.HTTPError`

        """

        plans = await self._dmptool_plans()
        return [DmptoolFileMetadata(plan) for plan in plans]

    async def _file_metadata(self, path):

        plan_md = await self._dmptool_plan(path)
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
            return (await self._file_metadata(path.path))

    async def download(self, path, **kwargs):
        """ Interface to downloading files from Dmptool

        :param path: Path mapping to waterbutler interpretation of Dmptool file
        :type path: `waterbutler.core.path.WaterButlerPath`
        :returns:  `waterbutler.core.streams.ResponseStreamReader` Download stream generator
        :raises:   `waterbutler.core.exceptions.DownloadError`
        """

        # # modeling after gdoc provider
        # # https://github.com/CenterForOpenScience/waterbutler/blob/develop/waterbutler/providers/googledrive/provider.py#L181-L185

        # print ("download: {path}".format(path=path))

        try:
            plan_id = path.parts[1].raw
            pdf = await self._dmptool_plan_pdf(plan_id)
        except Exception as e:
            # TO DO: throw the correct exception
            raise exceptions.DownloadError(
                'Could not retrieve file \'{0}\''.format(path),
                code=404,)

        stream = streams.StringStream(pdf)
        stream.content_type = 'application/pdf'
        stream.name = '{}.pdf'.format(plan_id)

        return stream

    async def validate_path(self, path, **kwargs):
        """
        :param path: Path to either a package or file.
        :type path: `str`
        """

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
        https://dmptool.org/api/v1/plans:
        For a list of plans: This API request requires authentication.
        Note: this call will return information for all plans created by users at your institution.
        "Private" plans will only be displayed if created after 09-26-2016 or if you are the plan owner.

        https://dmptool.org/api/v1/plans/:id:
        For a specific plan
        This API request requires authentication.
        """

        if id_ is None:
            resp = await self.get_url_async('plans')
            r = await resp.json()
            result = self._unroll(r)
        else:
            resp = await self.get_url_async('plans/{}'.format(id_))
            r = await resp.json()
            result = r.get('plan')

        return result

    async def plans_full(self, id_=None, format_='json'):

        """
        For a list of plans with all related attributes

        This API request requires authentication.
        Note: this call will return information for all plans created by users at your institution.
        "Private" plans will only be displayed if created after 09-26-2016 or if you are the plan owner.
        Without authentication, this call returns all public plans for all institutions.
        """

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

    async def plans_owned(self, filter_visibility=('test',)):
        """
        For a list of plans owned or co-owned by a user
        by default, filter out plans with test visibility
        """

        resp = await self.get_url_async('plans_owned')
        r = await resp.json()
        unrolled_plans = self._unroll(r)

        return [plan for plan in unrolled_plans
          if plan.get('visibility') not in filter_visibility]

    def plans_owned_full(self):
        """
        For a list of plans and all related attributes owned or co-owned by a user
        """
        return self._unroll(self.get_url('plans_owned_full').json())

    async def _dmptool_plans(self):

        plans = await self.plans_owned()

        # TO DO: try to compute actual length
        results = [{'title': plan['name'],
                  'guid': str(plan['id']),
                  'created': timestamp_iso(plan['created']),
                  'updated': timestamp_iso(plan['modified']),
                  'length': 0}
                  for plan in plans]

        return results

    async def _dmptool_plan(self, plan_id):

        try:
            plan = await self.plans(id_=plan_id)
        except Exception as e:
            # TO raise the proper exception?
            raise e
        else:
            result = {'title': plan['name'],
                  'guid': str(plan['id']),
                  'created': timestamp_iso(plan['created']),
                  'updated': timestamp_iso(plan['modified']),
                  'length': 0,
                  'content': ''}
            return result

    async def _dmptool_plan_pdf(self, plan_id):

        print("_dmptool_plan_pdf: {plan_id}".format(plan_id=plan_id))
        return await self.plans_full(plan_id, 'pdf')
