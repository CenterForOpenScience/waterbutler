from waterbutler.core import metadata
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.fedora import settings

# The raw data is a list of JSON_LD resources parsed as JSON.
# The resource_index is an index of the raw data keyed by '@id' normalized by having any trailing slashes stripped.
# The fedora_id is the id of the JSON_LD resource which the metadata resource represents.
# The wb_path is WaterButlerPath of resource.


class BaseFedoraMetadata(metadata.BaseMetadata):
    # The resource_index is only recalculated if not given.
    def __init__(self, raw, fedora_id, wb_path, resource_index=None):
        super().__init__(raw)
        self.fedora_id = fedora_id
        self.wb_path = wb_path
        self.resource_index = resource_index

        if self.resource_index is None:
            self.resource_index = {o['@id'].rstrip('/'): o for o in raw}

    @property
    def path(self):
        return '/' + self.wb_path.full_path

    @property
    def name(self):
        return self.wb_path.name

    @property
    def provider(self):
        return 'fedora'

    @property
    def modified(self):
        return self._get_property(settings.LAST_MODIFIED_PROPERTY_URI)

    @property
    def modified_utc(self):
        return self._get_property(settings.LAST_MODIFIED_PROPERTY_URI)

    @property
    def created_utc(self):
        return self._get_property(settings.CREATED_PROPERTY_URI)

    # Return an resource from index
    def _get_resource(self, resource_id):
        resource_id = resource_id.rstrip('/')

        if resource_id not in self.resource_index:
            raise exceptions.MetadataError('Could not find resource {} in JSON-LD'.format(resource_id), code=404)

        return self.resource_index[resource_id]

    # Return first value or raise exception
    def _get_property(self, property_uri):
        for o in self._get_resource(self.fedora_id).get(property_uri, []):
            if '@value' in o:
                return o['@value']

        raise exceptions.MetadataError('Could not find property {} in {}'.format(property_uri, self.fedora_id), code=404)


class FedoraFolderMetadata(BaseFedoraMetadata, metadata.BaseFolderMetadata):
    def list_children_metadata(self, repo_id):
        return [self._create_child_metadata(child_id, repo_id) for child_id in self._list_children()]

    def _list_children(self):
        return [o['@id'] for o in self._get_resource(self.fedora_id).get(settings.CONTAINS_PROPERTY_URI, [])]

    def _create_child_metadata(self, child_id, repo_id):
        # Derive the WaterButlerPath from the fedora ids being sure to handle its / requirements

        if child_id.startswith(repo_id):
            child_path_str = '/' + child_id[len(repo_id):].strip('/')
        else:
            raise exceptions.MetadataError('Child {} not in repository {}'.format(child_id, repo_id), code=404)

        child_obj = self._get_resource(child_id)

        if settings.CONTAINER_TYPE_URI in child_obj['@type']:
            return FedoraFolderMetadata(self.raw, child_id, WaterButlerPath(child_path_str + '/'), self.resource_index)
        else:
            return FedoraFileMetadata(self.raw, child_id, WaterButlerPath(child_path_str), self.resource_index)


class FedoraFileMetadata(BaseFedoraMetadata, metadata.BaseFileMetadata):
    @property
    def name(self):
        val = self._get_property(settings.FILENAME_PROPERTY_URI)

        return val if val else self.wb_path.name

    @property
    def size(self):
        return self._get_property(settings.SIZE_PROPERTY_URI)

    @property
    def content_type(self):
        return self._get_property(settings.MIME_TYPE_PROPERTY_URI)

    @property
    def etag(self):
        return None


# For the moment revisions are not supported
class FedoraFileRevisionMetadata(metadata.BaseFileRevisionMetadata):

    def __init__(self):
        pass

    @property
    def modified(self):
        return None

    @property
    def modified_utc(self):
        return None

    @property
    def version_identifier(self):
        return 'revision'

    @property
    def version(self):
        return 'latest'
