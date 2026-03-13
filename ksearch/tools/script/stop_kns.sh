#!/bin/sh
#Created on 2026-03-13
# kns test

# modify kns
echo "modify kns\n"
curl -d '{
    "op_type":"OP_KNS_STOP",
    "kns_info":{
        "kns_name": "TEST_KNS",
        "status": "KNS_STOP"
    }
}' http://$1/MetaService/meta_manager
echo "\n"
# query kns
curl -d '{
    "op_type" : "QUERY_KNS"
}' http://$1/MetaService/query
echo "\n"

