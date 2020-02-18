from waterbutler.core import metadata
from waterbutler.providers.figshare import settings


class BaseFigshareMetadata(metadata.BaseMetadata):

    @property
    def provider(self):
        return 'figshare'


class FigshareFileMetadata(BaseFigshareMetadata, metadata.BaseFileMetadata):

    def __init__(self, raw, raw_file=None):
        super().__init__(raw)
        if raw_file:
            self.raw_file = raw_file
        else:
            self.raw_file = self.raw['files'][0]

    @property
    def id(self):
        return self.raw_file['id']

    @property
    def name(self):
        return self.raw_file['name']

    @property
    def article_id(self):
        return self.raw['id']

    @property
    def article_name(self):
        if settings.ARTICLE_TYPE_IDENTIFIER in self.raw['url']:
            return ''
        return self.raw['title']

    @property
    def path(self):
        if settings.ARTICLE_TYPE_IDENTIFIER in self.raw['url']:
            return '/{0}'.format(self.id)
        return '/{0}/{1}'.format(self.article_id, self.id)

    @property
    def materialized_path(self):
        if settings.ARTICLE_TYPE_IDENTIFIER in self.raw['url']:
            return '/{0}'.format(self.name)
        # if self.raw['defined_type'] in settings.FOLDER_TYPES:
        #     return '/{0}/{1}'.format(self.article_name, self.name)
        # return '/{0}'.format(self.name)
        return '/{0}/{1}'.format(self.article_name, self.name)

    @property
    def upload_path(self):
        return self.path

    @property
    def content_type(self):
        return None

    @property
    def size(self):
        return self.raw_file['size']

    @property
    def modified(self):
        return None

    @property
    def created_utc(self):
        return None

    @property
    def etag(self):
        return '{}:{}:{}'.format(self.raw['status'].lower(),
                               self.article_id,
                               self.raw_file['computed_md5'])

    @property
    def is_public(self):
        """A property which indicates whether the article is public or not.

        The figshare "articles" endpoint now returns a dedicated field "is_public" to indicate if
        this article is public or not.  Every file in the article shares the same public/private
        attribute with its parent article.
        """
        return self.raw['is_public']

    @property
    def web_view(self):
        """A property which is a URL that let users view the article on the figshare website.

        The figshare "articles" endpoint now returns two dedicated fields for users to view the
        article on the figshare website, namely "url_private_html" and "url_public_html".  For all
        articles, both EXIST in the API response with a non-empty value.  The trick is, however,
        that both URLs WORK for public articles while only the former WORKS for private ones.
        """
        if self.is_public:
            return self.raw['url_public_html']
        else:
            return self.raw['url_private_html']

    @property
    def can_delete(self):
        """Files can be deleted if not public."""
        return (not self.is_public)

    @property
    def extra(self):
        return {
            'fileId': self.raw_file['id'],
            'articleId': self.article_id,
            'status': self.raw['status'].lower(),
            'downloadUrl': self.raw_file['download_url'],
            'canDelete': self.can_delete,
            'webView': self.web_view,
            'hashingInProgress': self.raw_file['status'] == 'ic_checking',
            'hashes': {
                'md5': self.raw_file['computed_md5'],
            },
        }


class FigshareFolderMetadata(BaseFigshareMetadata, metadata.BaseFolderMetadata):
    """Default config only allows articles of defined_type fileset to be
    considered folders.
    """

    @property
    def id(self):
        return self.raw['id']

    @property
    def name(self):
        return self.raw['title']

    @property
    def path(self):
        return '/{0}/'.format(self.raw.get('id'))

    @property
    def materialized_path(self):
        return '/{0}/'.format(self.name)

    @property
    def size(self):
        return None

    @property
    def modified(self):
        return self.raw['modified_date']

    @property
    def created_utc(self):
        return None

    @property
    def etag(self):
        return '{}::{}::{}'.format(self.raw['status'].lower(), self.raw.get('doi'), self.raw.get('id'))

    @property
    def extra(self):
        return {
            'id': self.raw.get('id'),
            'doi': self.raw.get('doi'),
            'status': self.raw['status'].lower(),
        }


class FigshareFileRevisionMetadata(metadata.BaseFileRevisionMetadata):

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
