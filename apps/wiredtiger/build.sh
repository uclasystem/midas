#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
MIDAS_DIR=$( cd -- $SCRIPT_DIR/../.. &> /dev/null && pwd )

rm -rf build
mkdir -p build
pushd build

cmake -DCMAKE_INCLUDE_PATH=${MIDAS_DIR}/bindings/c/include \
        -DCMAKE_LIBRARY_PATH=${MIDAS_DIR}/bindings/c/lib\
        -DENABLE_SHARED=0 -DENABLE_STATIC=1 \
        ../.

make -j

popd
