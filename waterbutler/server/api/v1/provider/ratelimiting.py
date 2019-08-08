import redis
import hashlib
import logging

from waterbutler.server import settings

logger = logging.getLogger(__name__)


class RateLimitingMixin:
    """ Rate-limiting WB API with Redis using the "Fixed Window" algorithm.
    """

    def __init__(self):

        # TODO: set different parameters for different types of auth
        self.WINDOW_SIZE = settings.RATE_LIMITING_FIXED_WINDOW_SIZE
        self.WINDOW_LIMIT = settings.RATE_LIMITING_FIXED_WINDOW_LIMIT
        self.redis_conn = redis.Redis(host=settings.REDIS_DOMAIN, port=settings.REDIS_PORT)

    def rate_limit(self):
        """ Check with the WB Redis server on whether to rate-limit a request.  Return True if the
        limit is reached; return False otherwise.
        """

        redis_key = self.get_auth_naive()
        logger.info('>>> RATE LIMITING >>> KEY >>> {}'.format(redis_key))

        # TODO: no need to check existence by calling the get since incr works on null keys
        counter = self.redis_conn.get(redis_key)
        if not counter:
            counter = self.redis_conn.incr(redis_key)
            self.redis_conn.expire(redis_key, self.WINDOW_SIZE)
            logger.info('>>> RATE LIMITING >>> NEW >>> key={} '
                        'counter={} url={}'.format(redis_key, counter, self.request.full_url()))
            return False
        counter = self.redis_conn.incr(redis_key)
        if counter > self.WINDOW_LIMIT:
            logger.info('>>> RATE LIMITING >>> FAIL >>> key={} '
                        'counter={} url={}'.format(redis_key, counter, self.request.full_url()))
            return True
        logger.info('>>> RATE LIMITING >>> PASS >>> '
                    'key={} counter={} url={}'.format(redis_key, counter, self.request.full_url()))
        return False

    def get_auth_naive(self):
        """ Get the authentication / authorization credentials from the request.

        Refer to ``tornado.httputil.HTTPServerRequest`` for more info on tornado's request object:
            https://www.tornadoweb.org/en/stable/httputil.html#tornado.httputil.HTTPServerRequest

        This is a NAIVE implementation in which Waterbutler rate-limiter only checks the existence
        of auth creds in the requests without further verifying them with the OSF.  Invalid creds
        will fail the next OSF auth part anyway even if it passes the rate-limiter.
        """

        osf_cookie = bearer_token = basic_creds = None

        # CASE 1: Requests with OSF cookies
        cookies = self.request.cookies or None
        if cookies and cookies.get('osf'):
            osf_cookie = cookies.get('osf').value

        auth_hdrs = self.request.headers.get('Authorization', None)
        if auth_hdrs:
            # CASE 2: Requests with OAuth bearer token
            bearer_token = auth_hdrs.split(' ')[1] if auth_hdrs.startswith('Bearer ') else None
            # CASE 3: Requests with basic auth using username and password
            basic_creds = auth_hdrs.split(' ')[1] if auth_hdrs.startswith('Basic ') else None

        # TODO: make sure the remote_ip is the real IP not our load balancers
        # CASE 4: Requests without any expected auth (case 1, 2 or 3 above).
        remote_ip = self.request.remote_ip or 'NOI.PNO.IPN.OIP'

        # TODO: allow cookie auth for free, loosely rate-limit token auth, disable support for basic
        #       username-password auth and strictly rate-limit requests with no auth.
        if bearer_token:
            logger.info('>>> RATE LIMITING >>> AUTH:TOKEN >>> {}'.format(bearer_token))
            return 'TOKEN__{}'.format(self._obfuscate_creds(bearer_token))
        if basic_creds:
            logger.info('>>> RATE LIMITING >>> AUTH:BASIC >>> {}'.format(basic_creds))
            return 'BASIC__{}'.format(self._obfuscate_creds(basic_creds))
        # SECURITY WARNING: Must check cookie last since it can only be allowed when used alone!
        if osf_cookie:
            logger.info('>>> RATE LIMITING >>> AUTH:COOKIE >>> {}'.format(osf_cookie))
            return 'COOKIE_{}'.format(self._obfuscate_creds(osf_cookie))
        logger.info('>>> RATE LIMITING >>> AUTH:NONE >>> {}'.format(remote_ip))
        return 'NOAUTH_{}'.format(self._obfuscate_creds(remote_ip))

    @staticmethod
    def _obfuscate_creds(creds):
        """Obfuscate authentication/authorization credentials: cookie, access token and password.

        It is not recommended to store the plain OSF cookie or the OAuth bearer token as key and it
        is evil to store the base64-encoded username and password as key since it is reversible.
        """

        return hashlib.sha256(creds.encode('utf-8')).hexdigest().upper()
