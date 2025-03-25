ARG IMAGESRC="el9"

FROM $IMAGESRC

# Allow username, UID, and GID to be overridden on
#  docker build command line:
#
ARG USER=scruser
ARG UID=1000
ARG GID=1000
ARG BASE_IMAGE


# Add configured user to image with sudo access:
#
RUN set -x && groupadd -g $UID $USER \
 && useradd -g $USER -u $UID -d /home/$USER -m $USER \
 && printf "$USER ALL= NOPASSWD: ALL\\n" >> /etc/sudoers

# Make sure user in appropriate group for sudo on different platforms
RUN case $BASE_IMAGE in \
     noble*)  adduser $USER sudo ;; \
     el*|fedora*) usermod -G wheel $USER ;; \
     *) (>&2 echo "Unknown BASE_IMAGE") ;; \
    esac

# Install extra dependencies if necessary here.
#
# Do not forget to run `apt update` on Ubuntu/jammy.
# Do NOT run `yum upgrade` on RPM systems (this will unnecessarily upgrade
#  existing packages)
#
RUN case $BASE_IMAGE in \
     jammy*) ;; \
     bookworm*) ;; \
     focal*) ;; \
     el*|fedora*) ;; \
     *) (>&2 echo "Unknown BASE_IMAGE") ;; \
    esac

ENV BASE_IMAGE=$BASE_IMAGE
USER $USER
WORKDIR /home/$USER
