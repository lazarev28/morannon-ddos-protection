import json
from slack_notif_ban import *


API_KEY = os.getenv('DDOS_CF_API_KEY', False)
EMAIL = os.getenv('DDOS_CF_EMAIL', False)
URL = "https://api.cloudflare.com/client/v4/zones/{}/firewall/access_rules/rules"


def action_ban_in_cf(asn, zone_id, logger, name_site, text_reason):
    url = URL.format(zone_id)
    data = {
        "configuration": {"target": "asn", "value": str(asn)},
        "mode": "block",
        "notes": f"AUTOBLOCK ASN {str(asn)}",
    }
    headers = {
        "X-Auth-Key": API_KEY,
        "X-Auth-Email": EMAIL,
        "Content-Type": "application/json"
    }
    response = requests.post(url, data=json.dumps(data), headers=headers)

    if response.status_code == 200:
        logger.info(f"Site {name_site}. ASN {asn} was successfully blocked.")
        slack_text = f'<!here> {name_site}. {text_reason}'
        action_notif_in_slack_about_ban(slack_text, logger)
    else:
        error_message = response.json().get('errors', [])[0].get('message', '')
        logger.error(f"Site {name_site}. ASN blocking error {asn}: {response.status_code}, {error_message}")

