FROM debian:11-slim

STOPSIGNAL SIGINT

ARG user=lbry
ARG db_dir=/database
ARG projects_dir=/home/$user

ARG DOCKER_TAG
ARG DOCKER_COMMIT=docker
ENV DOCKER_TAG=$DOCKER_TAG DOCKER_COMMIT=$DOCKER_COMMIT

RUN apt-get update && \
    apt-get -y --no-install-recommends install \
      wget \
      tar unzip \
      build-essential libssl-dev libffi-dev \
      automake libtool \
      pkg-config \
      python3.9 \
      python3.9-dev \
      python3-cffi \
      python3-pip  
      zstd && \
    update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 1 && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd -g 999 $user && useradd -m -u 999 -g $user $user
RUN mkdir -p $db_dir
RUN chown -R $user:$user $db_dir

COPY . $projects_dir
RUN chown -R $user:$user $projects_dir

USER $user
WORKDIR $projects_dir
RUN python3.9 -m pip install pip
RUN python3.9 -m pip install -e .
RUN python3.9 scripts/set_build.py
RUN rm ~/.cache -rf

# entry point
VOLUME $db_dir
ENV DB_DIRECTORY=$db_dir

COPY ./scripts/entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
