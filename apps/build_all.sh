#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

function build() {
    pushd $SCRIPT_DIR/$1
    make -j$(nproc)
    popd
}

build HDSearch
build storage
build synthetic

pushd $SCRIPT_DIR/wiredtiger
./build.sh
popd