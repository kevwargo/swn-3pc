#!/bin/bash

cd $(dirname $(realpath "$0"))

> hostfile
n="${1:-4}"
for i in `seq 1 $n`; do
    echo swn3pc_node_$i >> hostfile
done

./docker-compose -f docker-compose-single.yml up --scale node=$n
