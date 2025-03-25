FROM rockylinux:9

LABEL maintainer="James Corbett <corbett8@llnl.gov>"

#  Enable PowerTools for development packages
RUN dnf -y install 'dnf-command(config-manager)' \
 && dnf config-manager --enable crb \
#  Enable EPEL
 && dnf -y install epel-release \
#  Utilities
 && dnf -y groupinstall "Development Tools" \
 && dnf -y install \
	wget \
	man-db \
	less \
	git \
	sudo \
	valgrind \
	jq \
	which \
	file \
	vim \
	patch \
	diffutils \
#  Compilers, autotools
	libtool \
	gcc \
	gcc-c++ \
	clang \
	clang-tools-extra \
	make \
	ninja-build \
	cmake \
	gcc-c++ \
	mpi \
	mpi-devel \
	environment-modules \
	zlib-devel \
	pdsh \
#  Python
	python3-devel \
	mpich-devel \
 && dnf clean all

#  Set default /usr/bin/python to python3
RUN ln -s /usr/bin/python3 /usr/bin/python

ENV LANG=C.UTF-8
ENV MV2_SMP_USE_CMA=0
RUN printf "LANG=C.UTF-8" > /etc/locale.conf
