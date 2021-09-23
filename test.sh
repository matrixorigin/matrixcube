#!/bin/bash

for i in {1..50000}
do
    make test > test.log
    v=`tail -n 1 test.log | awk {'print $1'}`
    if [ "$v" != "ok" ]
    then
        mv ./test.log ./test-$i.log
        echo "$i: error"
    else
        echo "$i: ok"
    fi
done
