import redis
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler

load_dotenv()


from sentry_notif import *
from analyse_sites_logs import *


def convert_mb_to_bytes(mb):
    return mb * 1024 * 1024


MAX_BYTES = convert_mb_to_bytes(int(os.getenv('DDOS_MAX_MBYTES_LOGS', 0)))
BACKUP_COUNT = int(os.getenv('DDOS_BACKUP_COUNT_LOGS', 0))

handler = RotatingFileHandler("logs/analyse_sites_logs.log", maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT)
logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(levelname)s %(message)s", handlers=[handler])
LOGGER = logging.getLogger(__name__)



HOST = os.getenv('DDOS_REDIS_HOST', False)
PORT = os.getenv('DDOS_REDIS_PORT', False)
DB = os.getenv('DDOS_REDIS_DB', 0)
LOCK_KEY = "ddos_sites_analyse_logs"


def check_locked_cron_job():
    try:
        if not HOST:
            LOGGER.error('For Redis connection param HOST is empty')
            return
        if not PORT:
            LOGGER.error('For Redis connection param PORT is empty')
            return
        r = redis.StrictRedis(host=HOST, port=int(PORT), db=int(DB))
        is_locked = r.set(LOCK_KEY, "locked", nx=True)
        if is_locked:
            try:
                start_time = time.time()
                analyse_logs(LOGGER, sentry_sdk)
                end_time = time.time()
                elapsed_time = end_time - start_time
                LOGGER.info(f"Execution time for analyse sites logs: {elapsed_time:.2f} seconds")
            finally:
                r.delete(LOCK_KEY)
        else:
            return
    except Exception as e:
            sentry_sdk.capture_exception(e)


if __name__ == "__main__":
    check_locked_cron_job()
