#!/bin/bash
slaveHost=$1
masterHost=$2
fsPort=$3

ssh -t -o StrictHostKeyChecking=no ${slaveHost} "java -jar MapReduceFacility.jar -s ${masterHost} ${fsPort}"