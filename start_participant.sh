#!/bin/bash
args=("$@")
username=args[0]
slaveHost=args[1]
masterHost=args[2]

ssh ${username}@${masterHost}
./afs/andrew.cmu.edu/usr10/nbatliva
