FROM fedora:40

LABEL maintainer="James Corbett <corbett8@llnl.gov>"

ARG USER=scr
ARG UID=1000
ARG GID=1000

#  Enable PowerTools for development packages
#  Utilities
RUN dnf -y groupinstall "Development Tools" \
 && dnf -y install \
	wget \
	man-db \
	less \
	git \
	sudo \
	ccache \
	mpich \
	valgrind \
	jq \
	which \
	file \
	vim \
	patch \
	diffutils \
	hostname \
#  Compilers
	libtool \
	gcc \
	gcc-c++ \
	clang \
	clang-tools-extra \
	make \
	ninja-build \
	cmake \
	mpi \
	mpi-devel \
	environment-modules \
	zlib-devel \
	pdsh \
#  Python
	python3-devel \
#  Development dependencies
	valgrind-devel \
 && dnf clean all

ENV LANG=C.UTF-8
RUN printf "LANG=C.UTF-8" > /etc/locale.conf

# copy scripts into image
COPY src/test/docker/add_docker_user.sh /etc/add_docker_user.sh
# Add configured user to image with sudo access:
RUN /etc/add_docker_user.sh

USER $USER
WORKDIR /home/$USER
