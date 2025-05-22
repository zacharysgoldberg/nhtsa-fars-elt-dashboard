import os

SECRET_KEY = os.environ.get("SECRET_KEY", "default-insecure-key")

SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://superset:superset_pw@172.177.208.22:5434/superset'
