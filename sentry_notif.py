import sentry_sdk
import os

SENTRY_URL = os.getenv('DDOS_PATH_SITES_LOGS', False)

sentry_sdk.init(
    dsn=SENTRY_URL,
    traces_sample_rate=1.0,
    profiles_sample_rate=1.0,
)