#!/bin/sh
dir=./tests/$1
for i in `seq 1 $2`
do
    rm -rf /tmp/cube/*
    make all-tests > $dir/test.log
    v=`tail -n 1 $dir/test.log | awk {'print $1'}`
    if [ "$v" != "ok" ]
    then
        mv $dir/test.log $dir/test-$i.log
        echo "$i: error" >> $dir/result.log
    else
        echo "$i: ok" >> $dir/result.log
    fi
done