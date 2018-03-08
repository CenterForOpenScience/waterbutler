import re

from raven.processors import SanitizePasswordsProcessor


class WBSanitizer(SanitizePasswordsProcessor):
    """
    Use parent class to asterisk out things that look like passwords, credit card numbers,
    and API keys in frames, http, and basic extra data.

    In addition, asterisk out Dataverse formatted ouath tokens.
    """

    # Should specifically match Dataverse secrets. Key format checked on demo and on Harvard
    DATAVERSE_SECRET_RE = re.compile(r'[A-Za-z0-9]{8}-[A-Za-z0-9]{4}-[A-Za-z0-9]'
                                                '{4}-[A-Za-z0-9]{4}-[A-Za-z0-9]{12}')

    def __init__(self, client):
        super().__init__(client)
        # As of raven version 6.4 this attribute name has been changed from FIELDS to KEYS.
        # Will need to be updated when we upgrade.
        self.FIELDS = self.FIELDS.union(['key', 'token', 'refresh_token'])

    def sanitize(self, key, value):
        """Subclass the sanitize function of the `SanitizePasswordsProcessor'."""

        value = SanitizePasswordsProcessor.sanitize(self, key, value)

        if isinstance(value, dict):
            for item in value:
                value[item] = self.sanitize(item, value[item])

        if isinstance(value, list):
            new_list = []
            for item in value:
                new_list.append(self.sanitize(key, item))
            value = new_list

        # Check for Dataverse secrets
        if isinstance(value, str):
            matches = self.DATAVERSE_SECRET_RE.findall(value)
            for match in matches:
                value = value.replace(match, self.MASK)

        return value
