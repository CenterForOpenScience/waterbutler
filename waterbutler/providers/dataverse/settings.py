from waterbutler import settings

config = settings.child('DATAVERSE_PROVIDER_CONFIG')


EDIT_MEDIA_BASE_URL = config.get('EDIT_MEDIA_BASE_URL', "/dvn/api/data-deposit/v1.1/swordv2/edit-media/")
DOWN_BASE_URL = config.get('DOWN_BASE_URL', "/api/access/datafile/")
# TODO: double check and remove this unused API URL / endpoint
METADATA_BASE_URL = config.get('METADATA_BASE_URL', "/dvn/api/data-deposit/v1.1/swordv2/statement/study/")
JSON_BASE_URL = config.get('JSON_BASE_URL', "/api/v1/datasets/{0}/versions/:{1}")
