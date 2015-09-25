try:
    from waterbutler import settings
except ImportError:
    settings = {}

config = settings.get('OSF_AUTH_CONFIG', {})

JWT_EXPIRATION = config.get('JWT_EXPIRATION', 15)
JWT_ALGORITHM = config.get('JWT_ALGORITHM', 'HS256')
JWT_SECRET = config.get('JWT_SECRET', 'ILiekTrianglesALot')
API_URL = config.get('API_URL', 'http://127.0.0.1:5000/api/v1/files/auth/')
