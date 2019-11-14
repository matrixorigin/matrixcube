module github.com/deepfabric/beehive

go 1.13

require (
	github.com/coreos/etcd v3.3.17+incompatible
	github.com/deepfabric/elasticell v0.0.0-20191106005132-1d5f6b77d671
	github.com/deepfabric/prophet v0.0.0-20191104223059-f3dd066f0141
	github.com/fagongzi/goetty v1.3.1
	github.com/fagongzi/log v0.0.0-20191106015352-59d362b5908d
	github.com/fagongzi/util v0.0.0-20191031020235-c0f29a56724d
	github.com/gogo/protobuf v1.3.1
	github.com/google/btree v1.0.0
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8
	github.com/prometheus/client_golang v1.2.1
	github.com/shirou/gopsutil v2.19.9+incompatible
	github.com/stretchr/testify v1.4.0
	go.etcd.io/etcd v3.3.17+incompatible
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
)

replace github.com/coreos/etcd => github.com/deepfabric/etcd v3.3.17+incompatible
