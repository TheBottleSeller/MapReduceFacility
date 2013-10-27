#!/bin/bash
args=("$@")
slaveHost=args[0]
masterHost=args[1]
fsPort=args[2]

ssh -o StrictHostKeyChecking=no ${slaveHost}
java -jar Main.jar -s ${masterHost} ${fsPort}
