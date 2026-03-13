#!/bin/sh
#Created on 2026-03-13
# kns test

# drop kns
echo "drop kns\n"
curl -d '{
    "op_type":"OP_KNS_DROP_PEER",
    "kns_peer_info":{
        "kns_name": "TEST_KNS",
         "peer_name": "127.0.0.1:9000"
    }
}' http://$1/MetaService/meta_manager
echo "\n"
# query kns
curl -d '{
    "op_type" : "QUERY_KNS_PEER"
}' http://$1/MetaService/query
echo "\n"

