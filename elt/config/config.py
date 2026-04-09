from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv('MS_SQL_DB_NAME'),
    "user": os.getenv('MS_SQL_USER'),
    "password": os.getenv('MS_SQL_PASSWORD'),
    "host": os.getenv('MS_SQL_HOST'),
    "port": os.getenv('MS_SQL_PORT'),
}

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={DB_CONFIG['host']},{DB_CONFIG['port']};"
    f"DATABASE={DB_CONFIG['dbname']};"
    f"UID={DB_CONFIG['user']};"
    f"PWD={DB_CONFIG['password']};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)


AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
