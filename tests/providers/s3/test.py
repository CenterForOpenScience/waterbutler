# async def validate_v1_path(self, path, **kwargs):
#     await self._check_region()
#
#     # The user selected base folder, the root of the where that user's node is connected.
#     path = f"/{self.base_folder + path.lstrip('/')}"
#
#     implicit_folder = path.endswith('/')
#
#     if implicit_folder:
#         params = {'prefix': path, 'delimiter': '/'}
#         resp = await self.make_request(
#             'GET',
#             functools.partial(self.bucket.generate_url, settings.TEMP_URL_SECS, 'GET', query_parameters=params),
#             params=params,
#             expects=(200, 404,),
#             throws=exceptions.MetadataError,
#         )