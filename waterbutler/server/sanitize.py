import re

from raven.processors import SanitizePasswordsProcessor


class WBSanitizer(SanitizePasswordsProcessor):
    """Parent class asterisks out things that look like passwords, credit card numbers, and API
    keys in frames, http, and basic extra data.  This subclass will also filter values that look
    like Dataverse access tokens.
    """

    # Should specifically match Dataverse secrets. Key format checked on demo and on Harvard
    DATAVERSE_SECRET_RE = re.compile(r'[A-Za-z0-9]{8}-[A-Za-z0-9]{4}-[A-Za-z0-9]{4}-'
                                     '[A-Za-z0-9]{4}-[A-Za-z0-9]{12}')

    def __init__(self, client):
        super().__init__(client)
        # As of raven version 6.4 this attribute name has been changed from FIELDS to KEYS.
        # Will need to be updated when we upgrade.
        self.FIELDS = self.FIELDS.union(['key', 'token', 'refresh_token'])

    def sanitize(self, key, value):
        """Subclass the sanitize function of the `SanitizePasswordsProcessor'."""

        value = super().sanitize(key, value)

        if isinstance(value, dict):
            for item in value:
                value[item] = self.sanitize(item, value[item])

        elif isinstance(value, list):
            value = [self.sanitize(key, item) for item in value]

        elif isinstance(value, str):  # Check for Dataverse secrets
            value = self.DATAVERSE_SECRET_RE.sub(self.MASK, value)

        return value
