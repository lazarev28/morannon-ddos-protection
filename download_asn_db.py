import requests
import tarfile
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler

load_dotenv()


from sentry_notif import *


LICENSE_KEY = os.getenv('DDOS_ASN_LICENSE_KEY', False)
DB_PATH = os.getenv('DDOS_ASN_DB_PATH', False)

DOWNLOAD_URL = f'https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-ASN&license_key={LICENSE_KEY}&suffix=tar.gz'

TEMP_DIR = '~/geolite2_asn_update'


def convert_mb_to_bytes(mb):
    return mb * 1024 * 1024


MAX_BYTES = convert_mb_to_bytes(int(os.getenv('DDOS_DOWNLOAD_ASN_DB_MBYTES_LOGS', 0)))
BACKUP_COUNT = int(os.getenv('DDOS_DOWNLOAD_ASN_DB_COUNT_LOGS', 0))

handler = RotatingFileHandler("logs/asn_download_logs.log", maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT)
logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(levelname)s %(message)s", handlers=[handler])
logger = logging.getLogger(__name__)


def download_database():
    try:
        if not LICENSE_KEY:
            logger.error('LICENSE_KEY is empty')
            return
        if not DB_PATH:
            logger.error('DB_PATH is not selected')
            return
        os.makedirs(TEMP_DIR, exist_ok=True)
        temp_tar_path = os.path.join(TEMP_DIR, 'GeoLite2-ASN.tar.gz')
        response = requests.get(DOWNLOAD_URL, stream=True)
        if response.status_code == 200:
            with open(temp_tar_path, 'wb') as file:
                for chunk in response.iter_content(1024):
                    file.write(chunk)
            logger.info('The database has been successfully downloaded.')
        else:
            logger.info(f'Download error: {response.status_code}')
            return

        with tarfile.open(temp_tar_path, 'r:gz') as tar:
            for member in tar.getmembers():
                if member.name.endswith('GeoLite2-ASN.mmdb'):
                    member.name = os.path.basename(member.name)
                    tar.extract(member, TEMP_DIR)

        extracted_db_path = os.path.join(TEMP_DIR, 'GeoLite2-ASN.mmdb')
        if os.path.exists(extracted_db_path):
            os.replace(extracted_db_path, DB_PATH)
        else:
            return

        os.remove(temp_tar_path)
    except Exception as e:
            sentry_sdk.capture_exception(e)


if __name__ == '__main__':
    download_database()
