import os
import requests
import logging

from poseidon.util import general

conf = general.config


def send_email(to,
               subject,
               html_content,
               files=None,
               dryrun=False,
               cc=None,
               bcc=None,
               mime_subtype='mixed'):
    """Override airflow internal mail system. Notify via email."""
    # Sep by comma
    if conf['mg_notify'] == 1:
        request_url = 'https://api.mailgun.net/v3/{0}/messages'.format(conf[
            'mg_domain'])
        request = requests.post(
            request_url,
            auth=('api', conf['mg_key']),
            data={
                'from': conf['mg_from'],
                'to': conf['mg_to'],
                'subject': subject,
                'html': html_content
            })
        logging.info("Dispatched email - " + subject)


def notify(context):
    """Dispatch payload notification."""
    # Check local and test mode
    task_instance = context['task_instance']
    payload = {
        "run_date": context['execution_date'].isoformat(),
        "dag_id": task_instance.dag_id,
        "task_id": task_instance.task_id,
        "test_mode": task_instance.test_mode,
        "try_number": task_instance.try_number,
        "duration": task_instance.duration,
        "state": task_instance.state,
        "operator": task_instance.operator,
        "job_id": task_instance.job_id
    }
    if conf['keen_notify'] == 1:
        notify_keen(payload)
        logging.info('Dispatched Keen Notification for task: ' + payload[
            'task_id'])


def notify_keen(payload):
    url = 'https://api.keen.io/3.0/projects/{}/events/{}'.format(
        conf['keen_project_id'], conf['keen_ti_collection'])

    headers = {
        'Authorization': conf['keen_write_key'],
        'Content-Type': 'application/json'
    }

    request = requests.post(url, headers=headers, json=payload)
