#default imports
from datetime import timedelta, datetime
from pytz import timezone, utc
from dateutil.relativedelta import relativedelta
import os, sys, json, requests, random, logging, requests, argparse, calendar, psycopg2
import pandas as pd
import logging
from collections import defaultdict

from airflow import DAG
from airflow import models
from airflow.utils.db import provide_session
from airflow.models import Connection
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.slack_operator import SlackAPIPostOperator


APP_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(APP_DIR)
print(f'ROOT DIR -> {ROOT_DIR} & APP_DIR -> {APP_DIR}')
sys.path.append(APP_DIR)
sys.path.append(ROOT_DIR)
sys.path.append('/'.join(ROOT_DIR.split('/')[:-1]))

from common_lib.utils_v1 import *

BASE_URL = configuration.get('webserver', 'BASE_URL')
logging.basicConfig(level=logging.INFO)

DAG_ID = '{{ dag_id }}'
DAG_CONFIG_NAME = 'config_' + DAG_ID

def get_config(param):
    return get_conn(DAG_CONFIG_NAME).extra_dejson.get(param)

day_dt_nodash=''
day_dt_dash=''

if ldate:
    try:
        logical_timestamp = datetime.strptime(ldate, '%Y-%m-%d')
        day_dt_nodash = logical_timestamp.strftime('%Y%m%d')
        day_dt_dash = logical_timestamp.strftime('%Y-%m-%d')
        print(f'Processing for given date: {day_dt_dash}')
    except:
        logging.info(f'***** {ldate} is not a valid date. Either keep ldate empty in the connection config ({DAG_CONFIG_NAME}) OR provide a proper date in YYYY-MM-DD format. DAG will run for yesterday date ****')

{% for param in params %}
{% if param['key'] == BQ_CONNECTION_ID %}
{{ param['key'] }} = 'default-service-account'
{% elif param['key'] == GCP_SERVICE_ACCOUNT_KEY %}
{{ param['key'] }} = extract_service_act_path(BQ_CONNECTION_ID)
{% else %}
{{ param['key'] }} = get_config({{ param['value'] }})
{% endif %}
{% endfor %}

params = {
{% for param in params %}
"{{ param['key'] }}" : {{ param['value'] }},
{% endfor %}
}

default_args = {
{% for def_arg in default_args %}
"{{ def_arg['key'] }}" : {{ def_arg['value'] }},
{% endfor %}
}

def check_catalog_service(yesterday_ds, ds, **kwargs):
    try:
        http = requests.session()
        CATALOG_SERVICE_API = 'https://i-dss-infrastructure-prod.appspot.com/event/v1/instances/event_time_range'
        table_names = defaultdict(lambda: False)

        today = datetime.strptime(yesterday_ds, '%Y-%m-%d')
        today = datetime(today.year, today.month, today.day)
        yesterday = today - relativedelta(days=1)

        table_list = kwargs["params"]["table_names"]

        r = http.get(CATALOG_SERVICE_API, stream=True,
                        params={"name": "TableLoaded", "start": yesterday.isoformat(),
                        "end": today.isoformat()})
        r.raise_for_status()
        for raw in r.iter_lines():
            raw = json.loads(raw.decode('utf-8'))
            if raw['instance_properties']['tablename'] in table_list and table_names[raw['instance_properties']['tablename']] is not True:
                table_names[raw['instance_properties']['tablename']] = True

        if all(value == True for value in table_names.values()):
            return kwargs["params"]["run_task"]
        else:
            return 'slack_notify_failed'
    except Exception as e:
        exc_tb = sys.exc_info()
        error_log = "Error: {} At line no: {}\n".format(str(e), exc_tb.tb_lineno)
        logging.info(error_log)
        return 'slack_notify_failed'


with DAG(dag_id=DAG_ID, schedule_interval={{ schedule_interval }}, default_args=default_args) as dag:

    start_task = DummyOperator(task_id='start_task', retries=2, depends_on_past=False)
    end_task = DummyOperator(task_id='end_task', trigger_rule=TriggerRule.ALL_SUCCESS)

    check_cs_events = BranchPythonOperator(
        task_id='check_cs_events',
        provide_context=True,
        python_callable=check_catalog_service,
        params={"table_names": {
                {% for event in events -%}
                event['tablename']: event['active'],
                {% endfor %},
                "run_task": "rm_dfp_ad_summ_day"},
        on_failure_callback=slack_failed_task,
    )

    rm_summ_table_data = BigQueryOperator(
        task_id='rm_summ_table_data',
        use_legacy_sql=False,
        allow_large_results=True,
        sql='sql/{{ rm_sql_filename }}.sql',
        params=params,
        bigquery_conn_id=BQ_CONNECTION_ID,
        on_failure_callback=slack_failed_task,
        depends_on_past=True
    )

    insert_summ_table_data = BigQueryOperator(
        task_id='insert_summ_table_data',
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        allow_large_results=True,
        sql='sql/{{ insert_sql_filename }}.sql',
        params=params,
        bigquery_conn_id=BQ_CONNECTION_ID,
        on_failure_callback=slack_failed_task,
        depends_on_past=True ,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    gen_cs_done_event = PythonOperator(
        task_id='gen_cs_done_event',
        provide_context=True,
        python_callable=airflow_job_event_task,
        op_kwargs={'gcp_project': GC_BQ_PROJECT,
                    'bq_dataset_name': GC_BQ_DATASET,
                    'hour_nbr' : 'NULL',
                    'entity_cd' : DAG_ID,
                    'event_nm' : '{{ cs_done_event_name }}',
                    'status_cd' : 'done',
                    'frequency_cd' : 'daily',
                    'message_desc' : '{{ cs_done_event_desc }}'
                    },
        retries=30,
        retry_delay=timedelta(minutes=5)
    )

    slack_notify_done = SlackAPIPostOperator(
        task_id='slack_notify_done',
        username= 'Airflow',
        token=get_slack_token(),
        channel=SLACK_CHANNEL,
        attachments=[{"color": "good", "text": "*<" + BASE_URL +
                    "/admin/airflow/graph?dag_id={{ dag_id }}|{{ dag_id }}> for {{ ds }} Success!*"}],
        text='',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    start_task >> check_cs_events >> rm_summ_table_data >> insert_summ_table_data >> gen_cs_done_event >> slack_notify_done >> end_task
