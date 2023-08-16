#!/bin/bash

rmmod koord
rm /dev/koord
insmod $(dirname $0)/build/koord.ko
mknod /dev/koord c 280 0
chmod uga+rwx /dev/koord

