FROM debian:bookworm

LABEL maintainer="James Corbett <corbett8@llnl.gov>"

ARG USER=scr
ARG UID=1000
ARG GID=1000

# Update pkg caches, install latest pkg utils and basics:
RUN apt-get update \
 && apt-get -qq install -y --no-install-recommends \
        apt-utils \
        wget \
# Utilities
 && apt-get -qq install -y --no-install-recommends \
        locales \
        ca-certificates \
        wget \
        man \
        git \
        ssh \
        sudo \
        vim \
        mpich \
        libmpich-dev \
        valgrind \
        jq \
# Compilers, autotools
        build-essential \
        make \
        cmake \
        clang-15 \
        clang-tools-15 \
        gcc-12 \
        g++-12 \
        gfortran \
        zlib1g \
        zlib1g-dev \
        mpi \
        pdsh \
 && rm -rf /var/lib/apt/lists/* \
 && apt-get -qq autoremove -y

RUN locale-gen en_US.UTF-8

ENV LANG=C.UTF-8

# copy scripts into image
COPY src/test/docker/add_docker_user.sh /etc/add_docker_user.sh
# Add configured user to image with sudo access:
RUN /etc/add_docker_user.sh

USER $USER
WORKDIR /home/$USER
