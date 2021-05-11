#!/bin/bash
#
# Generate all beehive protobuf bindings.
# Run from repository root.
#
set -e

# directories containing protos to be built
MOD="github.com/matrixorigin/matrixcube"
VENDOR_DIR=$(dirname "$(dirname "$(dirname "$PWD")")")/vendor
PB_DIR=$(dirname "$PWD")/pb

if [ ! -d "$VENDOR_DIR/$MOD" ]; then
  rm -rf $VENDOR_DIR/$MOD
fi

mkdir -p $VENDOR_DIR/$MOD/components/prophet
cp -R $PB_DIR $VENDOR_DIR/$MOD/components/prophet

DIRS="./metapb ./rpcpb"
for dir in ${DIRS}; do
	pushd ${dir}
		protoc  -I=.:$VENDOR_DIR --gogofast_out=plugins=grpc:.  *.proto
		goimports -w *.pb.go
	popd
done

rm -rf $VENDOR_DIR/$MOD