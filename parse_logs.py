import re
from datetime import datetime, timedelta
from parse_asn import *
import os

DDOS_SEE_LOGS_LAST_MINUTES = int(os.getenv('DDOS_SEE_LOGS_LAST_MINUTES', 0))


def get_by_date_logs(log_file_path=False, sentry_sdk=False, block_size=4096):
    data = []
    try:
        pattern = r'(?P<ip>\d{1,3}(?:\.\d{1,3}){3}) - - \[(?P<datetime>.*?)\]'
        current_datetime = datetime.utcnow()
        time_threshold = current_datetime - timedelta(minutes=DDOS_SEE_LOGS_LAST_MINUTES)
        with open(log_file_path, 'rb') as log_file:
            log_file.seek(0, os.SEEK_END)
            file_size = log_file.tell()
            block_end = file_size

            while block_end > 0:
                block_start = max(0, block_end - block_size)
                log_file.seek(block_start)
                block = log_file.read(block_end - block_start)
                block_end = block_start

                lines = block.splitlines()

                if block_start > 0:
                    lines = lines[1:]

                for line in reversed(lines):
                    log_line = line.decode('utf-8')
                    if 'request-exportxml.xml' not in log_line and 'wp-content/' not in log_line:
                        match = re.search(pattern, log_line)
                        if match:
                            ip = match.group('ip')
                            datetime_log_str = match.group('datetime')

                            datetime_log = datetime.strptime(datetime_log_str, '%d/%b/%Y:%H:%M:%S %z').replace(tzinfo=None)

                            if time_threshold <= datetime_log:
                                asn, organization = get_asn_by_ip(ip)
                                data.append([datetime_log, ip, asn, organization])
                            else:
                                return data
    except Exception as e:
        if sentry_sdk:
            sentry_sdk.capture_exception(e)

    return data
