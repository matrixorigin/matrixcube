#!/bin/bash
rm -rf $PWD/tests
docker system prune -f
for i in `seq 1 $1`
do
    mkdir -p $PWD/tests/$i/tmp
    docker run -e RACE=1 -e MallocNanoZone=0 -e MEMFS_TEST=1 -i --rm --name=cube-test-$i -v  $PWD/tests/$i/tmp:/tmp -v $PWD:/matrixcube matrixorigin/matrixcube-test $i $2 &
done
