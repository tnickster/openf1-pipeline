from airflow.sdk import dag, task
from urllib.request import urlopen
import json
from datetime import datetime
from google.cloud import storage

@dag(
     schedule = "@monthly",
     start_date = datetime(2026, 3, 26),
     catchup = False
)

def calendar_dag():
     
     @task
     def fetch_meetings():
        response = urlopen(f"https://api.openf1.org/v1/meetings?year={datetime.now().year}")
        data = json.loads(response.read().decode('utf-8'))

        filtered = []
        for d in data:
            if "Testing" not in d["meeting_name"]:
                filtered.append(d)

        meetings = []

        for d in filtered:
            meeting = {
                "meeting_key": d["meeting_key"],
                "date_start": d["date_start"],
                "date_end": d["date_end"]
            }
            meetings.append(meeting)
        
        return meetings
     
     @task
     def upload_to_gcs(meetings):
         
         storage_client = storage.Client()
         bucket = storage_client.bucket("openf1-pipeline-raw")
         blob = bucket.blob(f"calendar/meetings_{datetime.now().year}.json")
         blob.upload_from_string(json.dumps(meetings), content_type="application/json")

     meetings = fetch_meetings()
     upload_to_gcs(meetings)

calendar_dag()

