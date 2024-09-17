import time
from parse_logs import *
import pandas as pd
from cloudflare_ban import *


PATH_SITES_LOGS = os.getenv('DDOS_PATH_SITES_LOGS', False)
DDOS_MAX_REQUESTS_FOR_ONE_MINUTE = int(os.getenv('DDOS_MAX_REQUESTS_FOR_ONE_MINUTE', 0))
COLUMNS = ['Date and Time', 'Ip Address', 'ASN', 'Organisation']


def read_json_file(file_path, sentry, logger):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return data
    except Exception as e:
        sentry.capture_exception(e)
        logger.error(f'Failed read file {file_path}. Error {e}')
        return []


def is_correct_url_path(site_url):
    if site_url.endswith('/'):
        return site_url
    else:
        return '{}/'.format(site_url)


def is_correct_url_file_path():
    if PATH_SITES_LOGS.startswith('/'):
        return PATH_SITES_LOGS[1:]
    else:
        return PATH_SITES_LOGS


def get_logs_from_site(site_url, site_name, logger):
    site_url = is_correct_url_path(site_url)
    main_url = site_url + PATH_SITES_LOGS
    try:
        response = requests.get(main_url)
        response.raise_for_status()
        file_content = response.text
        os.makedirs('files', exist_ok=True)
        file_path = f"files/{site_name}_logs.log"
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(file_content)
        logger.info(f"Logs successfully retrieved from {site_name} and saved to {file_path}")
        return True, file_path
    except requests.RequestException as e:
        logger.error(f"An error occurred while retrieving the file from {site_name}: {e}")
        return False, False


def filtered_and_group_data_logs(data_logs, logger, sentry):
    try:
        df = pd.DataFrame(data_logs, columns=COLUMNS)
        df['Date and Time'] = pd.to_datetime(df['Date and Time'])
        df['Minute'] = df['Date and Time'].dt.floor('min')
        grouped = df.groupby(['ASN', 'Minute']).size().reset_index(name='Count')
        filtered = grouped[grouped['Count'] > DDOS_MAX_REQUESTS_FOR_ONE_MINUTE]
        return filtered
    except Exception as e:
        sentry.capture_exception(e)
        return []



def analyse_logs(logger, sentry):
    if PATH_SITES_LOGS:
        file_path = "sites_data.json"
        data = read_json_file(file_path, sentry, logger)
        if data:
            for site in data.get('sites', []):
                site_name = site.get('name', False)
                site_url = site.get('url', False)
                logger.info(f"Started get logs from {site_name}")
                start_time = time.time()
                result_file, file_path = get_logs_from_site(site_url, site_name, logger)
                end_time = time.time()
                elapsed_time = end_time - start_time
                if result_file:
                    logger.info(f"Success get log file from {site_name}. Time {elapsed_time:.2f} seconds")
                    site.update({'file_path': file_path})
                else:
                    logger.error(f"Failed get log file from {site_name}")
        sites_with_file_path = [site for site in data.get('sites', []) if 'file_path' in site]
        if sites_with_file_path:
            if not os.getenv('DDOS_ASN_DB_PATH', False):
                logger.error(f"DDOS_ASN_DB_PATH is empty")
                return
            if not DDOS_MAX_REQUESTS_FOR_ONE_MINUTE:
                logger.error(f"DDOS_MAX_REQUESTS_FOR_ONE_MINUTE is empty")
                return
            sites_with_file_path_sorted = sorted(sites_with_file_path, key=lambda site: os.path.getsize(site['file_path']), reverse=True)
            for site in sites_with_file_path_sorted:
                file = site.get('file_path')
                file_size = os.path.getsize(file)
                zone_id = site.get('cf_zone_id', False)
                logger.info(f"Processing file: {file} (Size: {file_size} bytes)")
                start_time = time.time()
                data_logs = get_by_date_logs(file, sentry)
                if data_logs:
                    filtered_data = filtered_and_group_data_logs(data_logs, logger, sentry)
                    if len(filtered_data) > 0:
                        for index, row in filtered_data.iterrows():
                            asn = row["ASN"]
                            time_minute = row["Minute"]
                            count_requests = row["Count"]
                            text = f'В "{time_minute}" ASN "{asn}" зробив "{count_requests}" запитів в хвилину'
                            logger.warning(f'File {file} ' + text)

                            if not zone_id:
                                logger.error(f"For {file} zone_id is empty")
                            else:
                                action_ban_in_cf(asn, zone_id, logger, site.get('name', ''), text)
                end_time = time.time()
                elapsed_time = end_time - start_time
                minutes = os.getenv('DDOS_SEE_LOGS_LAST_MINUTES', 0)
                logger.info(
                    f"File {file}. In the last {minutes} minutes number of logs {len(data_logs)}. Time {elapsed_time:.2f} seconds")

    else:
        logger.error(f"PATH SITES LOGS is empty")
