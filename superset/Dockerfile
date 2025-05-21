FROM apache/superset:latest

USER root
RUN pip install psycopg2-binary Pillow
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
USER superset

EXPOSE 8088

ENTRYPOINT ["/entrypoint.sh"]
