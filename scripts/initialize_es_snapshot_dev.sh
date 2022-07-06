#!/bin/bash

ES_NAME="es01"
SNAPSHOT_HEIGHT="1188203"
SNAPSHOT_NODES="snapshot_es_${SNAPSHOT_HEIGHT}/snapshot/nodes"
ES_VOLUME_PATH="/var/lib/docker/volumes/${USER}_${ES_NAME}"

sudo rm -rf "${ES_VOLUME_PATH}/_data"
sudo mkdir -p "${ES_VOLUME_PATH}/_data"
sudo cp -r "${SNAPSHOT_NODES}" "${ES_VOLUME_PATH}/_data/"

sudo chown -R $USER:root "${ES_VOLUME_PATH}/_data"
sudo chmod -R 775 "${ES_VOLUME_PATH}/_data"
