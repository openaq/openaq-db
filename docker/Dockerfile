FROM postgres:13 as pg

LABEL maintainer="OpenAQ"

ENV POSTGIS_MAJOR 3

# defaults that may be overwritten by env
ENV PGUSER postgres
ENV PGDATABASE postgres
ENV PGHOST localhost
ENV POSTGRES_USER postgres
ENV POSTGRES_DB postgres
ENV POSTGRES_PASSWORD postgres

COPY --chown=postgres:postgres openaqdb/ /setup/

RUN \
    apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        unzip \
        apt-transport-https \
        debian-archive-keyring \
        jq \
        software-properties-common \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR \
        postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts \
        build-essential \
        python3 \
        python3-pip \
        python3-setuptools \
        git \
    && pip3 install -U pip setuptools packaging migra[pg] \
    && curl -L https://packagecloud.io/timescale/timescaledb/gpgkey | apt-key add - \
    && echo "deb https://packagecloud.io/timescale/timescaledb/debian/ buster main" > /etc/apt/sources.list.d/timescale.list \
    && apt-get update && apt-get -y --no-install-recommends install timescaledb-2-postgresql-13 timescaledb-tools \
    && apt-get remove -y apt-transport-https software-properties-common build-essential python3-pip python3-setuptools \
    && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 5432

RUN mkdir -p /docker-entrypoint-initdb.d \
    && echo "#!/bin/bash" >/docker-entrypoint-initdb.d/001_initdb.sh \
    && echo "timescaledb-tune --quiet --yes" >>/docker-entrypoint-initdb.d/001_initdb.sh \
    && echo "/setup/init.sh" >>/docker-entrypoint-initdb.d/001_initdb.sh


WORKDIR /openaqdb