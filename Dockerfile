FROM debian:stretch

MAINTAINER Even Rouault "even.rouault@spatialys.com"

WORKDIR "/app"

ENV EXTRACT_SERVICE_LOCALE_DIR "/locale"

RUN apt-get update && \
    apt-get install -y \
        python3-all-dev \
        python3-pip \
        python3-gdal  \
        python3-yaml \
        libldap2-dev \
        libsasl2-dev \
    && \
    rm -rf /var/lib/apt/lists/*

COPY resources /

RUN pip3 install -r /requirements.txt
RUN pip3 install uwsgi 

COPY frontend /app

EXPOSE 5000

RUN chmod +x /docker-entrypoint.sh /docker-entrypoint.d/*

RUN mkdir /extracts && \
    groupadd --gid 999 www && \
    useradd -r -ms /bin/bash --uid 999 --gid 999 www && \
    chown www:www /extracts

VOLUME ["/extracts"]

USER www

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["uwsgi", "--http", "0.0.0.0:5000", "--callable", "app", "--module", "app", "--chdir", "/app", "--uid", "www"]
