#!/bin/bash
#
# Generate all beehive protobuf bindings.
# Run from repository root.
#
set -e

# directories containing protos to be built
MOD="github.com/deepfabric/beehive"
DIRS="./bhmetapb ./bhraftpb ./raftcmdpb ./errorpb"
VENDOR_DIR=$(dirname "$PWD")/vendor
PB_DIR=$(dirname "$PWD")/pb

if [ ! -d "$VENDOR_DIR/$MOD" ]; then
  rm -rf $VENDOR_DIR/$MOD
fi

mkdir -p $VENDOR_DIR/$MOD
cp -R $PB_DIR $VENDOR_DIR/$MOD

mv $VENDOR_DIR/go.etcd.io/etcd/raft/raftpb/raft.proto $VENDOR_DIR/raft.proto.bak
cat $VENDOR_DIR/raft.proto.bak > $VENDOR_DIR/go.etcd.io/etcd/raft/raftpb/raft.proto
sed -i 's|gogoproto/gogo.proto|github.com/gogo/protobuf/gogoproto/gogo.proto|g' $VENDOR_DIR/go.etcd.io/etcd/raft/raftpb/raft.proto
for dir in ${DIRS}; do
	pushd ${dir}
		protoc  -I=.:$VENDOR_DIR --gogofast_out=plugins=grpc:.  *.proto
		sed -i 's/m.Raft.MarshalToSizedBuffer/m.Raft.MarshalTo/g' *.pb.go
		sed -i 's/m.HardState.MarshalToSizedBuffer/m.HardState.MarshalTo/g' *.pb.go
		goimports -w *.pb.go
	popd
done
rm -rf $VENDOR_DIR/go.etcd.io/etcd/raft/raftpb/raft.proto
mv $VENDOR_DIR/raft.proto.bak $VENDOR_DIR/go.etcd.io/etcd/raft/raftpb/raft.proto
rm -rf $VENDOR_DIR/$MOD