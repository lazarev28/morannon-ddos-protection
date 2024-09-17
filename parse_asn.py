import geoip2.database
import os

DB_PATH = os.getenv('DDOS_ASN_DB_PATH', False)

def get_asn_by_ip(ip):
    with geoip2.database.Reader(DB_PATH) as reader:
        response = reader.asn(ip)
        return response.autonomous_system_number, response.autonomous_system_organization

