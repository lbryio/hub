#!/bin/bash

# entrypoint for scribe Docker image

set -euo pipefail

SNAPSHOT_URL="${SNAPSHOT_URL:-}" #off by default. latest snapshot at https://lbry.com/snapshot/hub

if [[ "$HUB_COMMAND" == "scribe" ]] && [[ -n "$SNAPSHOT_URL" ]] && [[ ! -d /database/lbry-rocksdb ]]; then
  files="$(ls)"
  echo "Downloading hub snapshot from $SNAPSHOT_URL"
  wget --no-verbose --trust-server-names --content-disposition "$SNAPSHOT_URL"
  echo "Extracting snapshot..."
  filename="$(grep -vf <(echo "$files") <(ls))" # finds the file that was not there before
  case "$filename" in
    *.tgz|*.tar.gz|*.tar.bz2 )  tar xvf "$filename" --directory /database ;;
    *.zip ) unzip "$filename" -d /database/ ;;
    * ) echo "Don't know how to extract ${filename}. SNAPSHOT COULD NOT BE LOADED" && exit 1 ;;
  esac
  rm "$filename"
fi

if [ -z "$HUB_COMMAND" ]; then
  echo "HUB_COMMAND env variable must be scribe, herald, or scribe-elastic-sync"
  exit 1
fi

case "$HUB_COMMAND" in
  scribe ) exec /home/lbry/.local/bin/scribe "$@" ;;
  herald ) exec /home/lbry/.local/bin/herald "$@" ;;
  scribe-elastic-sync ) exec /home/lbry/.local/bin/scribe-elastic-sync "$@" ;;
  * ) "HUB_COMMAND env variable must be scribe, herald, or scribe-elastic-sync" && exit 1 ;;
esac
