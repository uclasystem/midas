#!/bin/bash

DISK_NAME=disk.bin
DISK_SIZE=16G

fallocate -l ${DISK_SIZE} ${DISK_NAME}