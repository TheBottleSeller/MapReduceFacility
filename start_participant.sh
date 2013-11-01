#!/bin/bash
slaveHost=$1
masterHost=$2
id=$3
port=$4

ssh -t -o StrictHostKeyChecking=no ${slaveHost} "java -jar MapReduceFacility.jar -s ${masterHost} ${id} ${port}"