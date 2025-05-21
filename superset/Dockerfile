FROM apache/superset:latest

USER root

RUN pip install psycopg2-binary Pillow gevent

COPY ./superset/entrypoint.sh /entrypoint.sh
COPY ./superset/superset_config.py /app/pythonpath/superset_config.py

RUN chmod +x /entrypoint.sh

USER superset

EXPOSE 8088

ENTRYPOINT ["/entrypoint.sh"]
