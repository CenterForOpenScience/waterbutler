import hashlib
import logging
from datetime import datetime, timedelta

from redis import Redis
from redis.exceptions import RedisError

from waterbutler.server import settings
from waterbutler.core.exceptions import WaterButlerRedisError

logger = logging.getLogger(__name__)


class RateLimitingMixin:
    """ Rate-limiting WB API with Redis using the "Fixed Window" algorithm.
    """

    def __init__(self):

        self.WINDOW_SIZE = settings.RATE_LIMITING_FIXED_WINDOW_SIZE
        self.WINDOW_LIMIT = settings.RATE_LIMITING_FIXED_WINDOW_LIMIT
        self.redis_conn = Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT,
                                password=settings.REDIS_PASSWORD)

    def rate_limit(self):
        """ Check with the WB Redis server on whether to rate-limit a request.  Returns a tuple.
        First value is `True` if the limit is reached, `False` otherwise.  Second value is the
        rate-limiting metadata (nbr of requests remaining, time to reset, etc.) if the request was
        rate-limited.
        """

        limit_check, redis_key = self.get_auth_naive()
        logger.debug('>>> RATE LIMITING >>> check={} key={}'.format(limit_check, redis_key))
        if not limit_check:
            return False, None

        try:
            counter = self.redis_conn.incr(redis_key)
        except RedisError:
            raise WaterButlerRedisError('INCR {}'.format(redis_key))

        if counter > self.WINDOW_LIMIT:
            # The key exists and the limit has been reached.
            try:
                retry_after = self.redis_conn.ttl(redis_key)
            except RedisError:
                raise WaterButlerRedisError('TTL {}'.format(redis_key))
            logger.debug('>>> RATE LIMITING >>> FAIL >>> key={} '
                         'counter={} url={}'.format(redis_key, counter, self.request.full_url()))
            data = {
                'retry_after': int(retry_after),
                'remaining': 0,
                'reset': str(datetime.now() + timedelta(seconds=int(retry_after))),
            }
            return True, data
        elif counter == 1:
            # The key does not exist and `.incr()` returns 1 by default.
            try:
                self.redis_conn.expire(redis_key, self.WINDOW_SIZE)
            except RedisError:
                raise WaterButlerRedisError('EXPIRE {} {}'.format(redis_key, self.WINDOW_SIZE))
            logger.debug('>>> RATE LIMITING >>> NEW >>> key={} '
                         'counter={} url={}'.format(redis_key, counter, self.request.full_url()))
        else:
            # The key exists and the limit has not been reached.
            logger.debug('>>> RATE LIMITING >>> PASS >>> key={} '
                         'counter={} url={}'.format(redis_key, counter, self.request.full_url()))

        return False, None

    def get_auth_naive(self):
        """ Get the obfuscated authentication / authorization credentials from the request.  Return
        a tuple ``(limit_check, auth_key)`` that tells the rate-limiter 1) whether to rate-limit,
        and 2) if so, limit by what key.

        Refer to ``tornado.httputil.HTTPServerRequest`` for more info on tornado's request object:
            https://www.tornadoweb.org/en/stable/httputil.html#tornado.httputil.HTTPServerRequest

        This is a NAIVE implementation in which WaterButler rate-limiter only checks the existence
        of auth creds in the requests without further verifying them with the OSF.  Invalid creds
        will fail the next OSF auth part anyway even if it passes the rate-limiter.

        There are four types of auth: 1) OAuth access token, 2) basic auth w/ base64-encoded
        username/password, 3) OSF cookie, and 4) no auth.  The naive implementation checks each
        method in this order.  Only cookie-based auth is permitted to bypass the rate-limiter.
        This order does not care about the validity of the auth mechanism.  An invalid Basic auth
        header + an OSF cookie will be rate-limited according to the Basic auth header.

        TODO: check with OSF API auth to see how it deals with multiple auth options.
        """

        auth_hdrs = self.request.headers.get('Authorization', None)

        # CASE 1: Requests with a bearer token (PAT or OAuth)
        if auth_hdrs and auth_hdrs.startswith('Bearer '):  # Bearer token
            bearer_token = auth_hdrs.split(' ')[1] if auth_hdrs.startswith('Bearer ') else None
            logger.debug('>>> RATE LIMITING >>> AUTH:TOKEN >>> {}'.format(bearer_token))
            return True, 'TOKEN__{}'.format(self._obfuscate_creds(bearer_token))

        # CASE 2: Requests with basic auth using username and password
        if auth_hdrs and auth_hdrs.startswith('Basic '):  # Basic auth
            basic_creds = auth_hdrs.split(' ')[1] if auth_hdrs.startswith('Basic ') else None
            logger.debug('>>> RATE LIMITING >>> AUTH:BASIC >>> {}'.format(basic_creds))
            return True, 'BASIC__{}'.format(self._obfuscate_creds(basic_creds))

        # CASE 3: Requests with OSF cookies
        # SECURITY WARNING: Must check cookie last since it can only be allowed when used alone!
        cookies = self.request.cookies or None
        if cookies and cookies.get('osf'):
            osf_cookie = cookies.get('osf').value
            logger.debug('>>> RATE LIMITING >>> AUTH:COOKIE >>> {}'.format(osf_cookie))
            return False, 'COOKIE_{}'.format(self._obfuscate_creds(osf_cookie))

        # TODO: Work with DevOps to make sure that the `remote_ip` is the real IP instead of our
        #       load balancers.  In addition, check relevatn HTTP headers as well.
        # CASE 4: Requests without any expected auth (case 1, 2 or 3 above).
        remote_ip = self.request.remote_ip or 'NOI.PNO.IPN.OIP'
        logger.debug('>>> RATE LIMITING >>> AUTH:NONE >>> {}'.format(remote_ip))
        return True, 'NOAUTH_{}'.format(self._obfuscate_creds(remote_ip))

    @staticmethod
    def _obfuscate_creds(creds):
        """Obfuscate authentication/authorization credentials: cookie, access token and password.

        It is not recommended to store the plain OSF cookie or the OAuth bearer token as key and it
        is evil to store the base64-encoded username and password as key since it is reversible.
        """

        return hashlib.sha256(creds.encode('utf-8')).hexdigest().upper()
