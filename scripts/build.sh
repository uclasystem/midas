#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
SRC_DIR=$( cd -- $SCRIPT_DIR/.. &> /dev/null && pwd )

# Build the Midas static library, the Midas coordinator (daemon), and unit tests.
pushd $SRC_DIR
make -j$(nproc)
popd

# Build Midas C libraries and bindings
pushd $SRC_DIR/bindings/c
make -j
make install # this will install the lib into bindings/c/lib
popd
