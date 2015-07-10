FROM debian:jessie
MAINTAINER Ross Kukulinski <ross@getyodlr.com>

ENV LAST_UPDATED 7_7_2015

RUN apt-get -yqq update && \
    apt-get -yqq install build-essential protobuf-compiler python \
                     libprotobuf-dev libcurl4-openssl-dev \
                     libboost-all-dev libncurses5-dev \
                     libjemalloc-dev wget

WORKDIR /src

ADD . /src

RUN ./configure --allow-fetch
RUN make -j 1
RUN make install
