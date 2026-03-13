#!/bin/sh
#Created on 2026-03-13
# kns test

# modify kns
echo "modify kns\n"
curl -d '{
    "op_type":"OP_KNS_STOP_PEER",
    "kns_peer_info":{
        "kns_name": "TEST_KNS",
         "peer_name": "127.0.0.1:9000",
        "status": "KNS_PEER_STOP"
    }
}' http://$1/MetaService/meta_manager
echo "\n"
# query kns
curl -d '{
    "op_type" : "QUERY_KNS_PEER"
}' http://$1/MetaService/query
echo "\n"

