#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROOT_DIR=$SCRIPT_DIR

build_docker_image() {
    docker build -f Dockerfile-builder-2204 -t socialnet_buildbase:latest .
}

if [[ ! $1 ]]
then
    build_docker_image
    $ROOT_DIR/run/dev.sh compile
elif [[ $1 == image ]]
then
    build_docker_image
elif [[ $1 == socialnet ]]
then
    $ROOT_DIR/run/dev.sh compile
fi
