#!/bin/bash

cd $(dirname $(realpath "$0"))

wget https://github.com/docker/compose/releases/download/1.18.0/docker-compose-`uname -s`-`uname -m` -O docker-compose
chmod +x docker-compose

echo "$@" | tr ' ' '\n' > hostfile

./docker-compose -f docker-compose-multi.yml up
