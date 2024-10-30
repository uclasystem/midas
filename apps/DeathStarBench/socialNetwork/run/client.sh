#!/bin/bash

export SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export ROOT_DIR=$( cd -- "$SCRIPT_DIR/.." &> /dev/null && pwd )

CONTAINER_NAME=$( docker ps --filter "status=running" --format "{{.Names}}" | grep client )

cp ${ROOT_DIR}/build/src/Client/Client ${ROOT_DIR}/services/Client
echo "docker exec -it $CONTAINER_NAME /services/Client $1"
docker exec -it $CONTAINER_NAME /services/Client $1 2>&1 | tee midas.log
# docker exec -it socialnetwork-sn-client-1 /bin/bash
