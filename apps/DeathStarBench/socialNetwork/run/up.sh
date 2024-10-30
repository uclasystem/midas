#!/bin/bash


SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROOT_DIR=$( cd -- "$SCRIPT_DIR/.." &> /dev/null && pwd )

${SCRIPT_DIR}/cp_services.sh

docker-compose -f ${SCRIPT_DIR}/../docker-compose.yml up -d
