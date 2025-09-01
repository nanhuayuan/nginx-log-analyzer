# Superset configuration for nginx analytics

import os

# Redis configuration
REDIS_HOST = "redis"
REDIS_PORT = 6379

# PostgreSQL configuration for metadata  
SQLALCHEMY_DATABASE_URI = "postgresql://superset:superset_password@postgres:5432/superset"

# Cache configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_REDIS_PORT': REDIS_PORT,
    'CACHE_REDIS_DB': 1,
}

# Security
SECRET_KEY = 'your_secret_key_here_change_in_production'
WTF_CSRF_ENABLED = True

# Default row limit for SQL Lab
DEFAULT_SQLLAB_LIMIT = 1000
