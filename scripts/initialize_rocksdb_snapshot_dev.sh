#!/bin/bash

SNAPSHOT_HEIGHT="1188203"
HUB_VOLUME_PATH="/var/lib/docker/volumes/${USER}_lbry_rocksdb"

sudo rm -rf "${HUB_VOLUME_PATH}/_data"
sudo mkdir -p "${HUB_VOLUME_PATH}/_data"
sudo cp -r "snapshot_${SNAPSHOT_HEIGHT}/lbry-rocksdb" "${HUB_VOLUME_PATH}/_data/"
sudo touch "${HUB_VOLUME_PATH}/_data/scribe.log"
sudo touch "${HUB_VOLUME_PATH}/_data/scribe-elastic-sync.log"
sudo touch "${HUB_VOLUME_PATH}/_data/herald.log"
sudo cp "snapshot_${SNAPSHOT_HEIGHT}/es_info" "${HUB_VOLUME_PATH}/_data/"
sudo chown -R 999:999 "${HUB_VOLUME_PATH}/_data"
