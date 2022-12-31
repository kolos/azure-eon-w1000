import gzip
import os
import datetime
import logging
import requests
import re
import json

import azure.functions as func
from azure.storage.blob import BlobServiceClient, ContentSettings

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    eon_csv_data = load_eon_w1000_report_csv()
    eon_json_data = convert_eon_csv_to_json(eon_csv_data)
    store_json_blob(eon_json_data)

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

def load_eon_w1000_report_csv() -> str:
    eon_username = os.environ['EON_LOGIN']
    eon_password = os.environ['EON_PASS']
    eon_reportid = os.environ['EON_REPORTID']
    eon_report_start_month = os.environ['EON_REPORT_START_MONTH']
    eon_report_start_day = os.environ['EON_REPORT_START_DAY']

    base_url = "https://energia.eon-hungaria.hu/W1000/"
    login_url = "Account/Login"
    report_url = "ExportReport/Export"

    s = requests.Session()
    login_page = s.get(base_url + login_url).text
    token = re.findall("value=\"([a-zA-Z0-9_\-]{108})\"", login_page)
    
    if len(token) < 1:
        logging.error('token match err')
        return '[]'
    
    
    login_data = {
        "UserName": eon_username,
        "Password": eon_password,
        "__RequestVerificationToken": token[0]
    }
    login_response = s.post(base_url + login_url, data= login_data, allow_redirects=False)

    if login_response.status_code != 302:
        logging.error('login error')
        return '[]'

    today = datetime.date.today()
    this_year_start = today.replace(month=1, day=1)
    current_billing_date = datetime.date(year=today.year, month=int(eon_report_start_month), day=int(eon_report_start_day))
    if(current_billing_date > today):
        current_billing_date = current_billing_date.replace(year=today.year-1)
    
    since = min(this_year_start, current_billing_date)
    next_month_start = (today.replace(day=1) + datetime.timedelta(days=32)).replace(day=1)

    report_content = s.post(base_url + report_url, data={
        "reportId": eon_reportid,
        "since": since,
        "until": next_month_start,
        "decimalSeparator": ".",
        "viewtype": "3",
        "exportformat": "3",
        "includestatus": "true",
    })

    return report_content.text

def convert_eon_csv_to_json(csv_text: str) -> str:
    json_data = []

    tmp = dict()
    for line in csv_text.splitlines()[1:]:
        (pod_name, obis_name, reading_time, reading_value, reading_status) = line.split(";")

        reading_time_timestamp = datetime.datetime.strptime(reading_time, "%Y.%m.%d %H:%M:%S").timestamp()*1e3

        if(pod_name not in tmp):
            tmp[pod_name] = dict()  
            tmp[pod_name]["name"] = pod_name + " " + obis_name.replace("'", "")
            tmp[pod_name]["unit"] = "kWh"
            tmp[pod_name]["data"] = []

        data_row = [int(reading_time_timestamp), float(reading_value), reading_status]
        tmp[pod_name]["data"].append(data_row)

    for pod_name in tmp:
        json_data.append(tmp[pod_name])

    return json.dumps(json_data)

def store_json_blob(data: str):
    conn_str = os.environ['AzureWebJobsStorage']
    container = os.environ['OUTPUT_CONTAINER_NAME']
    blob = os.environ['OUTPUT_FILE_NAME']
    compressed_data = gzip.compress(data.encode('utf8'))

    blob_service_client = BlobServiceClient.from_connection_string(
        conn_str=conn_str
    )

    blob_client = blob_service_client.get_blob_client(
        container=container,
        blob=blob
    )

    content_settings = ContentSettings(
        content_type='application/json',
        content_encoding='gzip',
    )

    blob_client.upload_blob(
        compressed_data,
        overwrite=True,
        content_settings=content_settings
    )
