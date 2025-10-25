FROM alpine:3.18

LABEL maintainer="James Corbett <corbett8@llnl.gov>"

# Update pkg caches, install latest pkg utils and basics:
RUN apk add \
    coreutils \
    bash \
    sudo \
    shadow \
    gcc \
    g++ \
    gfortran \
    libtool \
    make \
    cmake \
    python3 \
    git \
    jq \
    valgrind \
    openmpi \
    openmpi-dev \
    openssh \
    pdsh \
    zlib

ENV LANG=C.UTF-8
