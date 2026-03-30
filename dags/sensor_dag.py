from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import json
from google.cloud import storage
from datetime import datetime, timezone, timedelta

@dag(
     schedule = "@daily",
     start_date = datetime(2026, 3, 26),
     catchup = False
)

def race_weekend_sensor():

    @task
    def check_race_date():
        client = storage.Client()
        bucket = client.bucket("openf1-pipeline-raw")

        blob = bucket.blob(f"calendar/meetings_{datetime.now().year}.json")

        json_string = blob.download_as_text()
        data_dict = json.loads(json_string)

        for data in data_dict:
            date_end = datetime.fromisoformat(data["date_end"]).date()
            yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)

            if date_end == yesterday:
                return data["meeting_key"]

    meeting_key = check_race_date()

    trigger = TriggerDagRunOperator(
        task_id="trigger_ingestion",
        trigger_dag_id="ingestion_dag",
        conf={"meeting_key": meeting_key},
    )

    meeting_key >> trigger

race_weekend_sensor()
