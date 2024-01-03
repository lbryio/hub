#!/bin/bash

# entrypoint for scribe Docker image

set -euo pipefail

SNAPSHOT_URL="${SNAPSHOT_URL:-}" #off by default. latest snapshot at https://lbry.com/snapshot/hub

if [[ "$HUB_COMMAND" == "scribe" ]] && [[ -n "$SNAPSHOT_URL" ]] && [[ ! -d /database/lbry-rocksdb ]]; then
  files="$(ls)"
  echo "Downloading and extracting hub snapshot from $SNAPSHOT_URL"
  #wget --no-verbose -c "$SNAPSHOT_URL" -O - | tar x -C /database
  curl "$SNAPSHOT_URL" | zstd -d | tar xf - -C /database
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
