from airflow.sdk import dag, task, get_current_context
import json
from google.cloud import storage, bigquery
from datetime import datetime, timezone, timedelta
from urllib.request import urlopen
import time
from urllib.error import HTTPError
from schemas import drivers_schema, location_schema, laps_schema, starting_grid_schema


@dag(
    schedule = None,
    start_date = datetime(2026, 3, 26),
    catchup = False
)

def ingestion_dag():

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def fetch_sessions():
        context = get_current_context()
        dag_run = context['dag_run']
        if dag_run.conf:
            meeting_key = dag_run.conf.get("meeting_key")
        else:
            raise ValueError("No meeting_key found in dag_run.conf")
        try:
            response = urlopen(f"https://api.openf1.org/v1/sessions?meeting_key={meeting_key}")
            data = json.loads(response.read().decode('utf-8'))
        except HTTPError as e:
            if e.code == 429:
                raise  # trigger retry
            elif e.code in (404, 422):
                print(f"No data available: {e}")
                return
            else:
                raise

        for d in data: #get session_key that corresponds to meeting_key, this will be our PK for the rest of the tabels
            if d["session_type"] == "Race":
                return {
                    "session_key": d["session_key"],
                    "meeting_key": d["meeting_key"]
                }

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def fetch_drivers(keys):

        #unpack keys
        meeting_key = keys["meeting_key"]
        session_key = keys["session_key"]

        try:
            response = urlopen(f"https://api.openf1.org/v1/drivers?session_key={session_key}")
            data = json.loads(response.read().decode('utf-8'))
        except HTTPError as e:
            if e.code == 429:
                raise  # trigger retry
            elif e.code in (404, 422):
                print(f"No data available: {e}")
                return
            else:
                raise

        #store driver info
        driver_info = []
        for d in data:
            drivers = {
                "driver_number": d["driver_number"],
                "full_name": d["full_name"],
                "team_name": d["team_name"],
                "team_colour": d["team_colour"],
                "headshot_url": d["headshot_url"],
                "session_key": d["session_key"],
                "meeting_key": d["meeting_key"]
            }
            driver_info.append(drivers)

        #upload to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket("openf1-pipeline-raw")
        blob = bucket.blob(f"raw/meetings={meeting_key}/session={session_key}/drivers.json")
        blob.upload_from_string(
            "\n".join(json.dumps(record) for record in driver_info),
            content_type="application/json"
        )

        bq_client = bigquery.Client(project="openf1-pipeline")
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=drivers_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        uri = f"gs://openf1-pipeline-raw/raw/meetings={meeting_key}/session={session_key}/drivers.json"
        load_job = bq_client.load_table_from_uri(uri, "openf1-pipeline.raw.drivers", job_config=job_config)
        load_job.result()

        return [d["driver_number"] for d in driver_info]


    @task(retries=3, retry_delay=timedelta(seconds=30))
    def fetch_laps(keys):

        #unpack keys
        meeting_key = keys["meeting_key"]
        session_key = keys["session_key"]

        try:
            response = urlopen(f"https://api.openf1.org/v1/laps?session_key={session_key}")
            data = json.loads(response.read().decode('utf-8'))
        except HTTPError as e:
            if e.code == 429:
                raise  # trigger retry
            elif e.code in (404, 422):
                print(f"No data available: {e}")
                return
            else:
                raise

        #store driver info
        laps_info = []
        for d in data:
            lap = {
                "lap_number": d["lap_number"],
                "driver_number": d["driver_number"],
                "lap_duration": d["lap_duration"],
                "date_start": d["date_start"],
                "is_pit_out_lap": d["is_pit_out_lap"],
                "duration_sector_1": d["duration_sector_1"],
                "duration_sector_2": d["duration_sector_2"],
                "duration_sector_3": d["duration_sector_3"],
                "session_key": d["session_key"],
                "meeting_key": d["meeting_key"]
            }
            laps_info.append(lap)

        #upload to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket("openf1-pipeline-raw")
        blob = bucket.blob(f"raw/meetings={meeting_key}/session={session_key}/laps.json")
        blob.upload_from_string(
            "\n".join(json.dumps(record) for record in laps_info),
            content_type="application/json"
        )

        bq_client = bigquery.Client(project="openf1-pipeline")
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=laps_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        uri = f"gs://openf1-pipeline-raw/raw/meetings={meeting_key}/session={session_key}/laps.json"
        load_job = bq_client.load_table_from_uri(uri, "openf1-pipeline.raw.laps", job_config=job_config)
        load_job.result()



    @task(retries=3, retry_delay=timedelta(seconds=30))
    def fetch_location(keys, driver_numbers):

        #unpack keys
        meeting_key = keys["meeting_key"]
        session_key = keys["session_key"]

        # then loop through each driver
        location_info = []
        for driver_number in driver_numbers:
            try:
                response = urlopen(f"https://api.openf1.org/v1/location?session_key={session_key}&driver_number={driver_number}")
                data = json.loads(response.read().decode('utf-8'))
            except HTTPError as e:
                if e.code == 429:
                    time.sleep(30)
                    raise
                elif e.code in (404, 422):
                    print(f"No location data for driver {driver_number}: {e}")
                    continue
                else:
                    raise
            for d in data:
                location = {
                    "date": d["date"],
                    "driver_number": d["driver_number"],
                    "x": d["x"],
                    "y": d["y"],
                    "z": d["z"],
                    "session_key": d["session_key"],
                    "meeting_key": d["meeting_key"]
                }
                location_info.append(location)
            time.sleep(1)  # avoid rate limiting between drivers

        #upload to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket("openf1-pipeline-raw")
        blob = bucket.blob(f"raw/meetings={meeting_key}/session={session_key}/location.json")
        blob.upload_from_string(
            "\n".join(json.dumps(record) for record in location_info),
            content_type="application/json"
        )

        bq_client = bigquery.Client(project="openf1-pipeline")
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=location_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        uri = f"gs://openf1-pipeline-raw/raw/meetings={meeting_key}/session={session_key}/location.json"
        load_job = bq_client.load_table_from_uri(uri, "openf1-pipeline.raw.location", job_config=job_config)
        load_job.result()

    
    @task(retries=3, retry_delay=timedelta(seconds=30))
    def fetch_starting_grid(keys):

        #unpack keys
        meeting_key = keys["meeting_key"]
        session_key = keys["session_key"]

        try:
            response = urlopen(f"https://api.openf1.org/v1/starting_grid?session_key={session_key}")
            data = json.loads(response.read().decode('utf-8'))
        except HTTPError as e:
            if e.code == 429:
                raise  # trigger retry
            elif e.code in (404, 422):
                print(f"No data available: {e}")
                return
            else:
                raise

        #store driver info
        starting_grid_info = []
        for d in data:
            starting_grid = {
                "driver_number": d["driver_number"],
                "position": d["position"],
                "lap_duration": d["lap_duration"],
                "session_key": d["session_key"],
                "meeting_key": d["meeting_key"]
            }
            starting_grid_info.append(starting_grid)

        #upload to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket("openf1-pipeline-raw")
        blob = bucket.blob(f"raw/meetings={meeting_key}/session={session_key}/starting_grid.json")
        blob.upload_from_string(
            "\n".join(json.dumps(record) for record in starting_grid_info),
            content_type="application/json"
        )

        bq_client = bigquery.Client(project="openf1-pipeline")
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=starting_grid_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        uri = f"gs://openf1-pipeline-raw/raw/meetings={meeting_key}/session={session_key}/starting_grid.json"
        load_job = bq_client.load_table_from_uri(uri, "openf1-pipeline.raw.starting_grid", job_config=job_config)
        load_job.result()


    keys = fetch_sessions()
    driver_numbers = fetch_drivers(keys)
    fetch_laps(keys)
    fetch_location(keys, driver_numbers)
    fetch_starting_grid(keys)
    

ingestion_dag()