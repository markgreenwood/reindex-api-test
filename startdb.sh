#!/bin/bash
RESPONSE=$(curl --fail -s -XGET localhost:9200)
if [ 7 -eq $? ]
then
    echo 'Starting elasticsearch...'
    ~/elasticsearch-6.2.2/bin/elasticsearch > ~/elasticsearch.log 2>&1 &
    until
        curl --silent --fail -XGET localhost:9200
    do
        printf '.'
        sleep 1
    done
else
    echo $RESPONSE
fi
