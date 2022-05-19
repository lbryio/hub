#!/usr/bin/env bash

# usage: deploy_scribe_dev.sh <host to update>
TARGET_HOST=$1

SCRIPTS_DIR=`dirname $0`

# build the image
docker build -t lbry/hub:development .
IMAGE=`docker image inspect lbry/hub:development | sed -n "s/^.*Id\":\s*\"sha256:\s*\(\S*\)\".*$/\1/p"`

# push the image to the server
ssh $TARGET_HOST docker image prune --force
docker save $IMAGE | ssh $TARGET_HOST docker load
ssh $TARGET_HOST docker tag $IMAGE lbry/hub:development

## restart the wallet server
ssh $TARGET_HOST SCRIBE_TAG="development" docker-compose up -d
