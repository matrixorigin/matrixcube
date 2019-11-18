#!/bin/bash
rm -rf /tmp/beehive/$1
mkdir -p  /tmp/beehive/$1

if [ "$1" = "1" ]
then
    ./main --name=n$1 --addr=127.0.0.1:6379  --raft-addr=127.0.0.1:1000$1 --rpc-addr=127.0.0.1:2000$1 --prophet-addr=127.0.0.1:9521 --prophet-urls-client=http://127.0.0.1:2371  --prophet-urls-peer=http://127.0.0.1:2381 --data=/tmp/beehive/$1
else
    ./main --name=n$1 --addr=127.0.0.1:637$1 --raft-addr=127.0.0.1:1000$1 --rpc-addr=127.0.0.1:2000$1 --prophet-addr=127.0.0.1:952$1 --prophet-urls-client=http://127.0.0.1:237$1  --prophet-urls-peer=http://127.0.0.1:238$1 --data=/tmp/beehive/$1 --prophet-addr-join=http://127.0.0.1:2371
fi
