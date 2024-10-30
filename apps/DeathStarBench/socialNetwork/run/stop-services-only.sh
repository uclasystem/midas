#!/bin/bash

service_containers=$(cat service_containers.txt)

for s in $service_containers
do
    docker stop $s &
done

wait
