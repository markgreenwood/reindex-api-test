#!/bin/bash
curl --fail -s -XGET localhost:9200
if [ 7 -eq $? ]
then
    echo 'Starting elasticsearch...'
    nohup ~/elasticsearch-6.2.2/bin/elasticsearch &
else
    echo 'Everything is OK'
fi
