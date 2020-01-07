FROM python:3.7.4-alpine3.10
COPY requirements.txt ./
RUN \
    apk add --no-cache postgresql-libs && \
    apk add --no-cache --virtual .build-deps gcc musl-dev libressl-dev libffi-dev postgresql-dev python3-dev g++ jpeg-dev zlib-dev libstdc++
ENV LIBRARY_PATH=/lib:/usr/lib
RUN \
    /usr/local/bin/python3 -m pip install cython numpy && \
    /usr/local/bin/python3 -m pip install --no-cache-dir -r requirements.txt && \
    apk --purge del .build-deps

# install kubctl
ADD https://storage.googleapis.com/kubernetes-release/release/v1.15.0/bin/linux/amd64/kubectl /usr/local/bin/kubectl
ENV HOME=/config
RUN set -x && \
    apk add --no-cache curl ca-certificates  && \
    chmod +x /usr/local/bin/kubectl && \
    \
    # Create non-root user (with a randomly chosen UID/GUI).
    adduser kubectl -Du 2342 -h /config && \
    \
    # Basic check it works.
    kubectl version --client


RUN apk add --no-cache py3-gunicorn git openssh bash
COPY ssh /root/.ssh
RUN \
    echo $(ls -a /root/.ssh) && \
    ssh-keyscan -t rsa bitbucket.org > /root/.ssh/known_hosts && \
    git clone git@bitbucket.org:lynxmd/lynx-be.git /root/lynx-be
WORKDIR /root
COPY guni.sh ./
CMD bash ./guni.sh
