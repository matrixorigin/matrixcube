#!/bin/bash
doTesting(){
    base=$1
    dir=$2
    for i in {1..50000}
    do
        docker run -e RACE=1 -e MallocNanoZone=0 -e MEMFS_TEST=1 -i -v $base:/matrixcube matrixorigin/matrixcube-test all-tests > $dir/test.log
        v=`tail -n 1 $dir/test.log | awk {'print $1'}`
        if [ "$v" != "ok" ]
        then
            mv $dir/test.log $dir/test-$i.log
            echo "$i: error" >> $dir/result.log
        else
            echo "$i: ok" >> $dir/result.log
        fi
    done
}

rm -rf $PWD/tests
for i in `seq 1 $1`
do
    mkdir -p $PWD/tests/$i
    doTesting $PWD $PWD/tests/$i &
done
