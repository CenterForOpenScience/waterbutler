import hashlib
import logging

from redis import Redis
from redis.exceptions import RedisError

from waterbutler.server import settings
from waterbutler.core.exceptions import WaterbutlerRedisError

logger = logging.getLogger(__name__)


class RateLimitingMixin:
    """ Rate-limiting WB API with Redis using the "Fixed Window" algorithm.
    """

    def __init__(self):

        # TODO: set different parameters for different types of auth
        self.WINDOW_SIZE = settings.RATE_LIMITING_FIXED_WINDOW_SIZE
        self.WINDOW_LIMIT = settings.RATE_LIMITING_FIXED_WINDOW_LIMIT
        self.redis_conn = Redis(host=settings.REDIS_DOMAIN, port=settings.REDIS_PORT)

    def rate_limit(self):
        """ Check with the WB Redis server on whether to rate-limit a request.  Return True if the
        limit is reached; return False otherwise.
        """

        limit_check, redis_key = self.get_auth_naive()
        logger.info('>>> RATE LIMITING >>> check={} key={}'.format(limit_check, redis_key))
        if not limit_check:
            return False, None

        try:
            counter = self.redis_conn.incr(redis_key)
        except RedisError:
            raise WaterbutlerRedisError('INCR {}'.format(redis_key))
        if counter > self.WINDOW_LIMIT:
            # The key exists and the limit has been reached.
            try:
                retry_after = self.redis_conn.ttl(redis_key)
            except RedisError:
                raise WaterbutlerRedisError('TTL {}'.format(redis_key))
            logger.info('>>> RATE LIMITING >>> FAIL >>> key={} '
                        'counter={} url={}'.format(redis_key, counter, self.request.full_url()))
            data = {'retry_after': int(retry_after), }
            return True, data
        elif counter == 1:
            # The key does not exist and `.incr()` returns 1 by default.
            try:
                self.redis_conn.expire(redis_key, self.WINDOW_SIZE)
            except RedisError:
                raise WaterbutlerRedisError('EXPIRE {} {}'.format(redis_key, self.WINDOW_SIZE))
            logger.info('>>> RATE LIMITING >>> NEW >>> key={} '
                        'counter={} url={}'.format(redis_key, counter, self.request.full_url()))
        else:
            # The key exists and the limit has not been reached.
            logger.info('>>> RATE LIMITING >>> PASS >>> key={} '
                        'counter={} url={}'.format(redis_key, counter, self.request.full_url()))
        return False, None

    def get_auth_naive(self):
        """ Get the obfuscated authentication / authorization credentials from the request.  Return
        a tuple ``(limit_check, auth_key)`` that tells the rate-limiter 1) whether to rate-limit,
        and 2) if so, limit by what key.

        Refer to ``tornado.httputil.HTTPServerRequest`` for more info on tornado's request object:
            https://www.tornadoweb.org/en/stable/httputil.html#tornado.httputil.HTTPServerRequest

        This is a NAIVE implementation in which Waterbutler rate-limiter only checks the existence
        of auth creds in the requests without further verifying them with the OSF.  Invalid creds
        will fail the next OSF auth part anyway even if it passes the rate-limiter.

        There are totally four types of auth: 1) OSF cookie, 2) OAuth access token, 3) basic auth w/
        base64-encoded username/password and 4) w/o any of the three aforementioned auth.  The naive
        implementation allows cookie auth to bypass the rate-limiter while limiting the rest three.

        Please note that the above exception for cookie auth only applies when it is the only the
        auth option provided by the request.  The priority order is: token > basic > cookie.

        TODO: check with OSF API auth to see how it deals with multiple auth options.
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

        # TODO: Work with DevOps to make sure that the `remote_ip` is the real IP instead of our
        #       load balancers.  In addition, check relevatn HTTP headers as well.
        # CASE 4: Requests without any expected auth (case 1, 2 or 3 above).
        remote_ip = self.request.remote_ip or 'NOI.PNO.IPN.OIP'

        if bearer_token:
            logger.info('>>> RATE LIMITING >>> AUTH:TOKEN >>> {}'.format(bearer_token))
            return True, 'TOKEN__{}'.format(self._obfuscate_creds(bearer_token))
        if basic_creds:
            logger.info('>>> RATE LIMITING >>> AUTH:BASIC >>> {}'.format(basic_creds))
            return True, 'BASIC__{}'.format(self._obfuscate_creds(basic_creds))
        # SECURITY WARNING: Must check cookie last since it can only be allowed when used alone!
        if osf_cookie:
            logger.info('>>> RATE LIMITING >>> AUTH:COOKIE >>> {}'.format(osf_cookie))
            return False, 'COOKIE_{}'.format(self._obfuscate_creds(osf_cookie))
        logger.info('>>> RATE LIMITING >>> AUTH:NONE >>> {}'.format(remote_ip))
        return True, 'NOAUTH_{}'.format(self._obfuscate_creds(remote_ip))

    @staticmethod
    def _obfuscate_creds(creds):
        """Obfuscate authentication/authorization credentials: cookie, access token and password.

        It is not recommended to store the plain OSF cookie or the OAuth bearer token as key and it
        is evil to store the base64-encoded username and password as key since it is reversible.
        """

        return hashlib.sha256(creds.encode('utf-8')).hexdigest().upper()
