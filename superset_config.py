# superset_config.py
FEATURE_FLAGS = {
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_RBAC": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}

CACHE_CONFIG = {
    'CACHE_TYPE': 'simple'
}

WEBDRIVER_BASEURL = "http://localhost:8088/"