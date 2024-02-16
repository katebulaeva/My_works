import os

from dotenv import load_dotenv


load_dotenv()

database = os.getenv('DATABASE')        # Database for SQL
host = os.getenv('HOST')

psql_user = os.getenv('PSQL_USER')                # User for SQL
psql_password = os.getenv('PSQL_PASSWORD')        # Password for SQL              # Host for SQL
psql_port = os.getenv('PSQL_PORT')      # Port for Postgresql
