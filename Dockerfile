FROM postgres:14.5

RUN apt-get update &&\
    apt-get install -y \
    postgresql-14-decoderbufs