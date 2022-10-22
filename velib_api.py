import requests
import psycopg2
import os
import sys
from datetime import datetime

DB_USER=os.environ.get("POSTGRES_USER")
DB_PWD=os.environ.get("POSTGRES_PASSWORD")
DB_DATABASE=os.environ.get("POSTGRES_DB")
DB_HOST=os.environ.get("DB_HOST")
DB_PORT=os.environ.get("PORT")

def get_pg_connection():
  return psycopg2.connect(
    database = DB_DATABASE,
    user = DB_USER,
    password = DB_PWD,
    host = DB_HOST,
    port = DB_PORT
)


def get_station_status_list():
  res = requests.get("https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json")
  res.raise_for_status()
  station_status_list = res.json().get("data").get("stations")
  print(f"Fetched {len(station_status_list)} records...")
  return station_status_list


def get_parsed_station_status_list(station_status_list):
  return [
    [
      ("station_id", station_status.get("station_id")),
      ("num_bikes_available", station_status.get("num_bikes_available")),
      ("num_bikes_available_mechanical", {k: v for d in station_status.get("num_bikes_available_types", []) for k, v in d.items()}.get("mechanical")),
      ("num_bikes_available_electric", {k: v for d in station_status.get("num_bikes_available_types", []) for k, v in d.items()}.get("ebike")),
      ("num_docks_available", station_status.get("num_docks_available")),
      ("is_installed", station_status.get("is_installed")),
      ("is_returning", station_status.get("is_returning")),
      ("is_renting", station_status.get("is_renting")),
      ("last_reported", datetime.fromtimestamp(station_status.get("last_reported")).strftime("%Y-%m-%d %H:%M:%S")),
    ] for station_status in station_status_list
  ]


def values_ddl(parsed_station_status):
  joined_values = "', '".join(str(item[1]) for item in parsed_station_status)
  return f"('{joined_values}')"


def column_names_ddl(parsed_station_status):
  return f"({', '.join(item[0] for item in parsed_station_status[0])})"


def get_insert_query(parsed_station_status_list):
  column_names = column_names_ddl(parsed_station_status_list)
  values = ", ".join(values_ddl(parsed_station_status) for parsed_station_status in parsed_station_status_list)
  return f"INSERT INTO station_status_history {column_names} VALUES {values};"


def insert_values_in_db(parsed_station_status_list):
  insert_query = get_insert_query(parsed_station_status_list)
  with get_pg_connection() as conn:
    with conn.cursor() as cur:
      print(f"running insert query : {insert_query[:1000]} ...")
      cur.execute(insert_query)

def main():
  print("Starting main...")
  station_status = get_station_status_list()
  parsed_station_status_list = get_parsed_station_status_list(station_status)
  insert_values_in_db(parsed_station_status_list)
  print("Main Done!")


def init():
  print("Starting init...")
  create_tables()
  print("Init Done!")
  main()


def create_tables():
  create_migration_table_queries = [
    "CREATE TABLE IF NOT EXISTS migration (num INT, time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP)",
    "INSERT INTO migration (num) VALUES (0)"
  ]

  get_migration_number_query = "SELECT MAX(num) from migration"

  migrations = [
  """
  CREATE TABLE station_status_history (
    station_id BIGINT, 
    num_bikes_available INT, 
    num_bikes_available_mechanical INT, 
    num_bikes_available_electric INT,
    num_docks_available INT,
    is_installed BOOLEAN,
    is_returning BOOLEAN,
    is_renting BOOLEAN,
    last_reported TIMESTAMP WITHOUT TIME ZONE
  )
  """
  ]

  with get_pg_connection() as conn:
    with conn.cursor() as cur:
      for query in create_migration_table_queries:
        print(f"running query : {query}")
        cur.execute(query)

      print(f"running query : {get_migration_number_query}")
      cur.execute(get_migration_number_query)
      mingration_number = cur.fetchall()[0][0]

      for query in migrations[mingration_number:]:
        print(f"running query : {query}")
        cur.execute(query)


if __name__ == "__main__":
  if len(sys.argv) > 1 and sys.argv[1] == "--init":
    init()
  else:
    main()
