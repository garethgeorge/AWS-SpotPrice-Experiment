#!/bin/bash
echo "Downloading /mnt/data/$1/$2 via rsync"
rsync -ave 'ssh -p 22' root@169.231.235.184:/mnt/data/$1/$2 data/$1:$2