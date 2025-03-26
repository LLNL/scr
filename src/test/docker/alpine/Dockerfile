FROM alpine:3.18

LABEL maintainer="James Corbett <corbett8@llnl.gov>"

ARG USER=scr
ARG UID=1000
ARG GID=1000

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

# copy scripts into image
COPY src/test/docker/add_docker_user.sh /etc/add_docker_user.sh
# Add configured user to image with sudo access:
RUN /etc/add_docker_user.sh

USER $USER
WORKDIR /home/$USER
