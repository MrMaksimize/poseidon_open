"""General.py."""
import os
import errno
import pandas as pd
import logging
import requests
import shutil
from datetime import datetime, timedelta
import subprocess
import csv

from airflow.models import Variable



def seven_days_ago():
    """Return the date seven days ago."""
    return datetime.combine(datetime.today() - timedelta(7),
                            datetime.min.time())


def today():
    """Return today's date."""
    return datetime.combine(datetime.today(), datetime.min.time())


def get_year(the_date=datetime.now()):
    """Get current year, or year for date passed."""
    return the_date.strftime("%Y")


def get_FY_year(the_date=datetime.now()):
    """Return Fiscal Year based on today's date."""
    if the_date.month > 6:
        return 'FY' + str(the_date.year - 2000) + '-' + str(the_date.year -
                                                            1999)
    else:
        return 'FY' + str(the_date.year - 2001) + '-' + str(the_date.year -
                                                            2000)


def get_prev_FY_year(the_date=datetime.now()):
    """!!! Only use for traffic_counts_jobs.py."""
    return 'FY' + str(the_date.year - 2001) + '-' + str(the_date.year - 2000)


def buildConfig(env):
    """Take the current environment, generate build configuration."""
    config = {
        'env': (env or 'local').upper(),
        'op_start': datetime.strptime(
            # set default to prevent errors
            Variable.get('AIRFLOW_TASKS_START_DATE', '2017-02-01'),
            '%Y-%m-%d'),
        'default_s3_conn_id': 's3data',
        'prod_data_dir': "/data/prod",
        'temp_data_dir': "/data/temp",
        'date_format_ymd': "%Y-%m-%d",
        'date_format_ymd_hms': "%Y-%m-%d %H:%M:%S",
        'dags_dir': "/poseidon/poseidon/dags",
        'dest_s3_bucket': os.environ.get('S3_DATA_BUCKET', 'datasd-dev'),
        'mg_notify': int(os.environ.get("MAILGUN_NOTIFY")),
        'mg_key': os.environ.get("MAILGUN_KEY"),
        'mg_domain': os.environ.get("MAILGUN_DOMAIN"),
        'mg_from': os.environ.get("MAILGUN_FROM"),
        'mg_to': os.environ.get("MAILGUN_TO"),
        'keen_notify': int(os.environ.get("KEEN_NOTIFY")),
        'keen_project_id': os.environ.get('KEEN_PROJECT_ID'),
        'keen_write_key': os.environ.get('KEEN_WRITE_KEY'),
        'keen_ti_collection': os.environ.get('KEEN_TI_COLLECTION'),
    }
    return config


config = buildConfig(os.environ.get('SD_ENV'))

# https://crontab.guru/
schedule = {
    'pd_cfs': "@daily",
    'pd_col': "@daily",
    'ttcs': "@daily",
    'indicator_bacteria_tests': "@daily",
    'parking_meters': "@daily",
    'traffic_counts': "@weekly",
    'read': "@daily",
    'dsd_approvals': "@daily",
    'dsd_code_enforcement': "@daily",
    'streets_sdif': "@daily",
    'streets_imcat': "@daily",
    'get_it_done': "@hourly",
    'gid_potholes': "@daily",
    'special_events': "@daily",
    'waze': "*/5 * * * *",  # every 5 minutes
    'inventory': "@monthly"  # Run 1x a month at 00:00 of the 1st day of mo
}

source = {'ttcs': os.environ.get('CONN_ORACLETTCS')}

args = {
    'owner': 'airflow',
    'start_date': config['op_start'],
    'depends_on_past': False,
    'email': config['mg_from'],
    'email_on_failure': config['mg_notify'],
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=120)
    #'on_failure_callback': notify,
    #'on_retry_callback': notify,
    #'on_success_callback': notify
    # TODO - on failure callback can be here,
    # TODO - look into sla
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


def create_path_if_not_exists(path):
    """Create path if it does not exist."""
    try:
        os.makedirs(path)
        logging.info('Created path at ' + path)
    except OSError as exc:  # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise
    return path


def pos_write_csv(df, fname, **kwargs):
    """Write csv file, creating paths as needed, with default confs."""
    default = {
        'index': False,
        'encoding': 'utf-8',
        'doublequote': True,
        'date_format': config['date_format_ymd'],
        'quoting': csv.QUOTE_ALL
    }
    csv_args = default.copy()
    csv_args.update(kwargs)
    try:
        os.makedirs(os.path.dirname(fname))
    except OSError as exc:  # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise

    df.to_csv(fname, **csv_args)


def file_to_string(rel_file_path, caller=None):
    """Read a file into a string variable.  Caller is __file___."""
    if caller:
        rel_file_path = expand_rel_path(caller, rel_file_path)

    with open(rel_file_path, 'r') as ftoread:
        fstring = ftoread.read()
    return fstring


def expand_rel_path(caller, rel_path):
    """Expand a relative path."""
    return os.path.join(os.path.dirname(os.path.realpath(caller)), rel_path)


def geocode_address_esri(address_line='', **kwargs):
    """Geocoding function using SANDAG geocoder."""
    # Type safe
    address_line = str(address_line)
    url = "http://gis1.sandag.org/sdgis/rest/services/REDI/REDI_COMPOSITE_LOC/GeocodeServer/findAddressCandidates"
    payload = {
        'City': 'San Diego',
        'SingleLine': address_line,
        'outSR': '4326',
        'f': 'pjson'
    }
    logging.info('ESRI Geocoding for: ' + address_line)
    if (address_line == 'None' or address_line == ''):
        logging.info('No geocode for: ' + address_line)
        return None, None
    else:
        r = requests.get(url, payload)
        resp = r.json()
        candidates = resp['candidates']
        if candidates == []:
            logging.info('No geocode for: ' + address_line)
            return None, None
        else:
            logging.info('Geocode success for: ' + address_line)
            return candidates[0]['location']['y'], \
                   candidates[0]['location']['x']
