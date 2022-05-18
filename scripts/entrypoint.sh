#!/bin/bash

# entrypoint for scribe Docker image

set -euo pipefail

if [ -z "$HUB_COMMAND" ]; then
  echo "HUB_COMMAND env variable must be scribe, herald, or scribe-elastic-sync"
  exit 1
fi

case "$HUB_COMMAND" in
  scribe ) exec /home/lbry/.local/bin/scribe "$@" ;;
  scribe-hub ) exec /home/lbry/.local/bin/herald "$@" ;;
  scribe-elastic-sync ) exec /home/lbry/.local/bin/scribe-elastic-sync "$@" ;;
  * ) "HUB_COMMAND env variable must be scribe, herald, or scribe-elastic-sync" && exit 1 ;;
esac
