FROM debian:stretch

MAINTAINER Even Rouault "even.rouault@spatialys.com"

ENV C_FORCE_ROOT true
ENV IDGO_EXTRACT_EXTRACTS_DIR "/extracts"
ENV EXTRACT_SERVICE_LOCALE_DIR "/locale"

RUN apt-get update && apt-get install -y gdal-bin python3-gdal python3 python3-pip python3-psycopg2 python3-yaml

# install requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt && rm /tmp/requirements.txt

# copy source code
COPY . /celery
WORKDIR /celery

RUN mkdir /extracts && \
    groupadd --gid 999 www && \
    useradd -r -ms /bin/bash --uid 999 --gid 999 www && \
    chown www:www /extracts

VOLUME ["/extracts"]

USER www

# run the worker
ENTRYPOINT ["celery", "-A", "extractions", "worker", "--loglevel=info", "--concurrency=1"]