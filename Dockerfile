FROM apache/superset:latest

COPY ./entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8088

ENTRYPOINT ["/entrypoint.sh"]
