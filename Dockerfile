#FROM postgis/postgis:16-3.5 as pg
FROM postgis/postgis:16-3.5 as pg

LABEL maintainer="OpenAQ"
ARG ADD_MOCK_DATA=false

# defaults that may be overwritten by env
ENV POSTGIS_MAJOR 3
ENV POSTGRESQL_MAJOR 16
ENV PGUSER postgres
ENV PGDATABASE postgres
ENV PGHOST localhost
ENV POSTGRES_DB postgres
ENV POSTGRES_USER postgres
ENV POSTGRES_PASSWORD postgres
ENV DATABASE_DB openaqdev
ENV DATABASE_READ_USER postgresread
ENV DATABASE_READ_PASSWORD postgresdev
ENV DATABASE_WRITE_USER postgreswrite
ENV DATABASE_WRITE_PASSWORD postgresdev

COPY --chown=postgres:postgres ./openaqdb/ /openaqdb/

EXPOSE 5432

 # Docker image will automatically run scripts in `/docker-entrypoint-initdb.d`
 RUN mkdir -p /docker-entrypoint-initdb.d \
     && echo "#!/bin/bash" >/docker-entrypoint-initdb.d/001_initdb.sh \
     && echo "/openaqdb/init.sh" >> /docker-entrypoint-initdb.d/001_initdb.sh


RUN if [ "$ADD_MOCK_DATA" = "true" ]; then \
    echo "/openaqdb/mock.sh" >> /docker-entrypoint-initdb.d/001_initdb.sh;  \
  fi

WORKDIR /openaqdb
