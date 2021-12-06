Rate-limiting
=============

As of the v21.2.0 release, WaterButler has built-in rate-limiting via redis. The implementation uses the fixed window algorithm.

Method
------

Users are distinguished first by their credentials and then by their IP address. The rate limiter recognizes different types of auth and rate-limits each type separately. The four recognized auth types are: OSF cookie, OAuth bearer token, basic auth with base64-encoded username/password, and un-authed.

OSF cookies, OAuth access tokens, and base64-encoded usernames/passwords are used as redis keys during rate-limiting. WB obfuscates them for the same reason that only password hashes are stored in a database. SHA-256 is used in this case. A prefix is also added to the digest to identify which type it is. The "No Auth" case is hashed as well (unnecessarily) so that the keys all have the same look and length.

Auth by OSF cookie currently bypasses the rate limiter to avoid throttling web users.

Configuration
-------------

Rate limiting settings are found in `waterbutler.server.settings`.  By default, WB allows 3600 requests per auth per hour.  Rate-limiting is turned OFF by default; set `ENABLE_RATE_LIMITING` to `True` turn it on.  The relevant envvars are:

* ``SERVER_CONFIG_ENABLE_RATE_LIMITING``: `Boolean`. Defaults to `False`.
* ``SERVER_CONFIG_REDIS_HOST``: The host redis is listening on. Default is ``'192.168.168.167'``.
* ``SERVER_CONFIG_REDIS_PORT``: The port redis is listening on. Default is ``'6379'``.
* ``SERVER_CONFIG_REDIS_PASSWORD``: The password for the configured redis instance. Default is `None`.
* ``SERVER_CONFIG_RATE_LIMITING_FIXED_WINDOW_SIZE``: Number of seconds until the redis key expires. Default is 3600s.
* ``SERVER_CONFIG_RATE_LIMITING_FIXED_WINDOW_LIMIT``: Number of reqests permitted while the redis key is active. Default is 3600.

Behavior
--------

Return the Retry-After header in the 429 response if the limit is hit.  This header states when it will be acceptable to send another request.  Other informative headers are included to provide context, though currently only after the rate limiting has been enforced.

If rate-limiting is enabled and WB is unable to reach redis, a 503 Service Unavailable error will be thrown.  Since redis is not expected to be available during ci, rate limiting is turned off.
