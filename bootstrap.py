import os
from string import Template

import airflow
from datetime import date, datetime, timedelta
from airflow.utils.db import *
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser


session = settings.Session()

#:https://github.com/apache/incubator-airflow/blob/master/airflow/utils/db.py

# Delete All Stock Connections'

print("REMOVING STOCK CONNECTIONS")
session.query(models.Connection).delete()
session.commit()

# Create Connections

bucket_no_dots_cf = 'boto.s3.connection.SubdomainCallingFormat'
bucket_has_dots_cf = 'boto.s3.connection.OrdinaryCallingFormat'

extra_tmpl = Template(
    '{"aws_access_key_id": "$access_key", "aws_secret_access_key": "$secret", "calling_format": "$calling_format"}'
)

print("ADDING LOG S3 Connection")
params = extra_tmpl.substitute(
    access_key=os.environ.get('CONN_S3_ACCESS_KEY', ''),
    secret=os.environ.get('CONN_S3_SECRET', ''),
    calling_format=bucket_no_dots_cf)

merge_conn(models.Connection(conn_id='s3log', conn_type='aws', extra=params))

print("ADDING DATA S3 Connection")
params = extra_tmpl.substitute(
    access_key=os.environ.get('CONN_S3_ACCESS_KEY', ''),
    secret=os.environ.get('CONN_S3_SECRET', ''),
    calling_format=bucket_has_dots_cf)

merge_conn(models.Connection(conn_id='s3data', conn_type='aws', extra=params))

print("CREATE AIRFLOW DEFAULT CONNECTION")
C = models.Connection
airflow_db_conn = session.query(C).filter(C.conn_id == 'airflow_db').first()

if airflow_db_conn is not None:
    session.delete(airflow_db_conn)
    session.commit()

merge_conn(
    models.Connection(
        conn_id='airflow_db',
        conn_type='postgres',
        host='postgres',
        login=os.environ.get('AIRFLOW_DB_USER'),
        password=os.environ.get('AIRFLOW_DB_PASS'),
        schema='airflow'))

# Set Variables

# TODO -- this may no longer be a good idea because of prod data persistence.
#af_tasks_start = datetime.now().strftime('%Y-%m-%d')
af_tasks_start = os.environ.get('AIRFLOW_TASKS_START_YMD', '2017-02-01')
print("SET RUN START DATE VARIABLE TO " + af_tasks_start)

models.Variable.set('AIRFLOW_TASKS_START_DATE', af_tasks_start)

# Create User
username = os.environ.get('AIRFLOW_USER', 'nunya')
current_admin = session.query(models.User).filter(models.User.username == username).first()

# Create user if not exists
if current_admin is None:
    user = PasswordUser(models.User())
    user.username = username
    user.email = os.environ.get('AIRFLOW_USER_EMAIL', 'nunya@datasd.org')
    user.password = os.environ.get('AIRFLOW_USER_PASS', 'nunya123')
    session.add(user)
    session.commit()

# Close Session
session.close()
exit()
