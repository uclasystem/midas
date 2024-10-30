#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROOT_DIR=$( cd -- "$SCRIPT_DIR/.." &> /dev/null && pwd )

MIDAS_DIR=$( cd -- "$ROOT_DIR/../../.." &> /dev/null && pwd )

IMAGE_NAME=socialnet_buildbase
CONTAINER_NAME=${IMAGE_NAME}

start_docker() {
    docker_running=$(docker ps --format '{{.Names}}' | grep ${CONTAINER_NAME})
    if [[ ! $docker_running ]]
    then
        docker run -d -it --name ${CONTAINER_NAME} \
            -v /dev/shm:/dev/shm \
            -v ${MIDAS_DIR}:/midas \
            -v ${ROOT_DIR}:${ROOT_DIR} \
            -v ${ROOT_DIR}/services:/services \
            -v ${ROOT_DIR}/config:/config \
            ${IMAGE_NAME} /bin/bash
    fi
}

into_docker() {
    docker exec -it ${CONTAINER_NAME} /bin/bash
}

compile_socialnet() {
    docker exec -it ${CONTAINER_NAME} /bin/bash -c "cd $ROOT_DIR && ./run/compile.sh"
}

launch_daemon() {
    docker exec -it ${CONTAINER_NAME} /bin/bash -c "cd /midas && ./scripts/run_daemon.sh"
}

if [[ ! $1 ]] || [[ $1 == start ]]
then
    start_docker
    into_docker
elif [[ $1 == stop ]]
then
    docker stop ${CONTAINER_NAME}
    docker rm ${CONTAINER_NAME}
elif [[ $1 == compile ]]
then
    start_docker
    compile_socialnet
elif [[ $1 == daemon ]]
then
    start_docker
    launch_daemon
fi
