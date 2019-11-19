#!/bin/bash
mockgen --destination=./mock_meta.go -source=./meta.go -self_package="github.com/deepfabric/prophet" -package="prophet" 
mockgen --destination=./mock_prophet_rpc.go -source=./prophet_rpc.go -self_package="github.com/deepfabric/prophet" -package="prophet" 
mockgen --destination=./mock_prophet.go -source=./prophet.go -self_package="github.com/deepfabric/prophet" -package="prophet" 
mockgen --destination=./mock_peer_store.go -source=./peer_store.go -self_package="github.com/deepfabric/prophet" -package="prophet"
mockgen --destination=./mock_peer_replica.go -source=./peer_replica.go -self_package="github.com/deepfabric/prophet" -package="prophet"