#!/bin/bash

pushd server
python3 server.py &
popd

redis-server --save "" --appendonly no --maxmemory-policy allkeys-lru &

sleep 5

./build/img2vec 1

pkill -9 redis-server
pkill -9 python3