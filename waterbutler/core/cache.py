import asyncio


class CacheableMetadataProviderProxy:
    """Wraps a provider so that folder ``metadata`` requests are cached and the
    metadata of any child folders is prefetched concurrently.

    When a folder is listed, a metadata task is scheduled for each of its child
    folders without awaiting them. Those tasks run on the event loop while files
    are being downloaded, so the generator can issue all of its metadata requests
    up front instead of fetching folder listings strictly one-at-a-time.

    Any attribute that is not overridden here (``download``, ``path_from_metadata``,
    etc.) is delegated to the wrapped provider.
    """

    def __init__(self, provider):
        self.provider = provider
        self.__cache = dict[str, asyncio.Task]()

    def metadata_task(self, path, **kwargs):
        key = path.identifier or str(path)
        if key not in self.__cache:
            self.__cache[key] = asyncio.create_task(
                self.provider.metadata(path, **kwargs)
            )
        return self.__cache[key]

    async def metadata(self, path, **kwargs):
        if path.is_file:
            return await self.provider.metadata(path, **kwargs)

        items = await self.metadata_task(path, **kwargs)

        for item in items:
            if item.is_folder:
                child = self.provider.path_from_metadata(path, item)
                self.metadata_task(child, **kwargs)

        return items

    def __getattr__(self, name):
        return getattr(self.provider, name)
