#!/bin/bash

# NOTE: must be called in the docker container

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROOT_DIR=$( cd -- "$SCRIPT_DIR/.." &> /dev/null && pwd )

rm -rf $ROOT_DIR/build
mkdir $ROOT_DIR/build
pushd $ROOT_DIR/build
cmake ..
make -j
popd