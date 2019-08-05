import redis
import logging

from waterbutler.server.settings import REDIS_DOMAIN, REDIS_PORT

logger = logging.getLogger(__name__)

"""
WB API Rate Limiting With Redis - Notes and TODOs

* Implement different algorithms
    * Choose during initialization or switch on-the-fly
    * Move each algorithm into its own class and hide details
    * Double check race conditions, missed limit, etc.

* Decide what and how to limit:
    * Request from OSF servers (users interact with WB via OSF)
    * Request from non-OSF servers (users interact with WB directly)
    * What KEY should we limit by? IP address, user auth info, User-Agent, etc.

* Performance VS Security
    * The rate depends on what we expect from the rate limiter.
    * How much is the performance overhead?

* Config redis properly
    * Use a config file
    * Secure the database
    * Secure the connection
    * Restrict the access
    * What if redis hang, error or fail to connect?

* Handle distributed scenario:
    * 3 WB servers for US and 2 WB servers for other regions
    * Take care of Load Balancers

* Return rate limiting info for every requests in headers
    * The Retry-After header
    * Customized Waterbutler headers for more rate-limiting details
"""


class RateLimitingMixin:

    def __init__(self):

        self.algorithm = 1  # use an integer to identify each algorithm
        self.algorithm_name = 'fixed_window'
        self.window_size = 3600  # 1 hour in seconds
        self.window_limit = 3600  # allow a max of 3600 requests per hour
        self.redis_conn = redis.Redis(host=REDIS_DOMAIN, port=REDIS_PORT)

    def rate_limit(self):
        """Check with the WB Redis server on whether to rate-limit a request.  Return True if the
        limit is reached; return False otherwise.
        """

        # Refer to tornado.httputil.HTTPServerRequest for more info on tornado's request object:
        # https://www.tornadoweb.org/en/stable/httputil.html#tornado.httputil.HTTPServerRequest
        remote_ip = self.request.remote_ip or "255.255.255.255"
        redis_key = remote_ip  # a trivial solution

        # TODO: no need to check existence by calling the get since incr works on null keys
        counter = self.redis_conn.get(redis_key)
        if not counter:
            counter = self.redis_conn.incr(redis_key)
            self.redis_conn.expire(redis_key, self.window_size)
            logger.info('>>> RATE LIMITING >>> NEW:'
                        '{} {} {}'.format(remote_ip, counter, self.request.full_url))
            return False
        counter = self.redis_conn.incr(redis_key)
        if counter > self.window_limit:
            logger.info('>>> RATE LIMITING >>> FAIL: '
                        '{} {} {}'.format(remote_ip, counter, self.request.full_url))
            return True
        logger.info('>>> RATE LIMITING >>> PASS: '
                    '{} {} {}'.format(remote_ip, counter, self.request.full_url))
        return False
