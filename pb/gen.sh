#!/bin/bash
#
# Generate all matrixcube protobuf bindings.
# Run from repository root.
#
set -e

# directories containing protos to be built
MOD="github.com/matrixorigin/matrixcube"
DIRS="./meta ./rpc ./errorpb ./metapb ./rpcpb"
VENDOR_DIR=$(dirname "$PWD")/vendor
PB_DIR=$(dirname "$PWD")/pb
# PROPHET_PB_DIR=$(dirname "$PWD")/pb

if [ ! -d "$VENDOR_DIR/$MOD" ]; then
  rm -rf $VENDOR_DIR/$MOD
fi

# mkdir -p $VENDOR_DIR/$MOD/components/prophet
cp -R $PB_DIR $VENDOR_DIR/$MOD
# cp -R $PROPHET_PB_DIR $VENDOR_DIR/$MOD/components/prophet

mv $VENDOR_DIR/go.etcd.io/etcd/raft/v3/raftpb/raft.proto $VENDOR_DIR/raft.proto.bak
cat $VENDOR_DIR/raft.proto.bak > $VENDOR_DIR/go.etcd.io/etcd/raft/v3/raftpb/raft.proto
# sed -i 's|gogoproto/gogo.proto|github.com/gogo/protobuf/gogoproto/gogo.proto|g' $VENDOR_DIR/go.etcd.io/etcd/raft/v3/raftpb/raft.proto
for dir in ${DIRS}; do
	pushd ${dir}
		protoc  -I=.:$VENDOR_DIR --gogofast_out=plugins=grpc:.  *.proto
		goimports -w *.pb.go
	popd
done
rm -rf $VENDOR_DIR/go.etcd.io/etcd/raft/v3/raftpb/raft.proto
mv $VENDOR_DIR/raft.proto.bak $VENDOR_DIR/go.etcd.io/etcd/raft/v3/raftpb/raft.proto
rm -rf $VENDOR_DIR/$MOD
