import os

from dotenv import load_dotenv


load_dotenv()

database = os.getenv('DATABASE')        # Database for SQL
host = os.getenv('HOST')

psql_user = os.getenv('PSQL_USER')                # User for SQL
psql_password = os.getenv('PSQL_PASSWORD')        # Password for SQL              # Host for SQL
psql_port = os.getenv('PSQL_PORT')      # Port for Postgresql



cities_coordinates = {
    "Moscow": {"latitude": 55.7558, "longitude": 37.6176},
    "Kazan": {"latitude": 55.8304, "longitude": 49.0661},
    "St. Petersburg": {"latitude": 59.9343, "longitude": 30.3351},
    "Tula": {"latitude": 54.2021, "longitude": 37.6177},
    "Novosibirsk": {"latitude": 55.0084, "longitude": 82.9357}
}
