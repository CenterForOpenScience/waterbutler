from waterbutler import settings

config = settings.child('OSF_AUTH_CONFIG')


JWT_EXPIRATION = int(config.get('JWT_EXPIRATION', 15))
JWT_ALGORITHM = config.get('JWT_ALGORITHM', 'HS256')
API_URL = config.get('API_URL', 'http://localhost:5000/api/v1/files/auth/')

JWE_SALT = config.get('JWE_SALT')
JWE_SECRET = config.get('JWE_SECRET')
JWT_SECRET = config.get('JWT_SECRET')

if not settings.DEBUG:
    assert JWE_SALT, 'JWE_SALT must be specified when not in debug mode'
    assert JWE_SECRET, 'JWE_SECRET must be specified when not in debug mode'
    assert JWT_SECRET, 'JWT_SECRET must be specified when not in debug mode'

JWE_SALT = (JWE_SALT or 'yusaltydough')
JWE_SECRET = (JWE_SECRET or 'CirclesAre4Squares')
JWT_SECRET = (JWT_SECRET or 'ILiekTrianglesALot')

MFR_ACTION_HEADER = config.get('MFR_ACTION_HEADER', 'X-Cos-Mfr-Request-Action')
