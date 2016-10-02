from waterbutler import settings

config = settings.get('GITHUB_PROVIDER_CONFIG')


BASE_URL = config.get('BASE_URL', 'https://api.github.com/')
VIEW_URL = config.get('VIEW_URL', 'https://github.com/')

MOVE_MESSAGE = config.get('MOVE_MESSAGE', 'Moved on behalf of WaterButler')
COPY_MESSAGE = config.get('COPY_MESSAGE', 'Copied on behalf of WaterButler')
DELETE_FILE_MESSAGE = config.get('DELETE_FILE_MESSAGE', 'File deleted on behalf of WaterButler')
UPDATE_FILE_MESSAGE = config.get('UPDATE_FILE_MESSAGE', 'File updated on behalf of WaterButler')
UPLOAD_FILE_MESSAGE = config.get('UPLOAD_FILE_MESSAGE', 'File uploaded on behalf of WaterButler')
DELETE_FOLDER_MESSAGE = config.get('DELETE_FOLDER_MESSAGE', 'Folder deleted on behalf of WaterButler')
