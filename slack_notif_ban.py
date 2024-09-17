import requests
import os

DDOS_SLACK_WEBHOOK_URL = os.getenv('DDOS_SLACK_WEBHOOK_URL', False)


def action_notif_in_slack_about_ban(text, logger):
    payload = {
        "text": text
    }
    response = requests.post(
        DDOS_SLACK_WEBHOOK_URL,
        json=payload
    )
    if response.status_code == 200:
        logger.info(f"Notification sent successfully: {text}")
    else:
        logger.error(f"Failed to send notification: {text}. Status code: {response.status_code}")