from google.cloud import bigquery

drivers_schema = [
bigquery.SchemaField("driver_number", "INTEGER"),
bigquery.SchemaField("full_name", "STRING"),
bigquery.SchemaField("team_name", "STRING"),
bigquery.SchemaField("team_colour", "STRING"),
bigquery.SchemaField("headshot_url", "STRING"),
bigquery.SchemaField("session_key", "INTEGER"),
bigquery.SchemaField("meeting_key", "INTEGER"),
bigquery.SchemaField("name_acronym", "STRING")
]
location_schema = [
bigquery.SchemaField("date", "TIMESTAMP"),
bigquery.SchemaField("driver_number", "INTEGER"),
bigquery.SchemaField("x", "INTEGER"),
bigquery.SchemaField("y", "INTEGER"),
bigquery.SchemaField("z", "INTEGER"),
bigquery.SchemaField("session_key", "INTEGER"),
bigquery.SchemaField("meeting_key", "INTEGER"),
]
laps_schema = [
bigquery.SchemaField("date_start", "TIMESTAMP"),
bigquery.SchemaField("driver_number", "INTEGER"),
bigquery.SchemaField("duration_sector_1", "FLOAT64"),
bigquery.SchemaField("duration_sector_2", "FLOAT64"),
bigquery.SchemaField("duration_sector_3", "FLOAT64"),
bigquery.SchemaField("is_pit_out_lap", "BOOL"),
bigquery.SchemaField("lap_duration", "FLOAT64"),
bigquery.SchemaField("lap_number", "INTEGER"),
bigquery.SchemaField("session_key", "INTEGER"),
bigquery.SchemaField("meeting_key", "INTEGER")
]
starting_grid_schema = [
bigquery.SchemaField("position", "INTEGER"),
bigquery.SchemaField("driver_number", "INTEGER"),
bigquery.SchemaField("lap_duration", "FLOAT64"),
bigquery.SchemaField("session_key", "INTEGER"),
bigquery.SchemaField("meeting_key", "INTEGER"),
]
