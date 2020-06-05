module github.com/deepfabric/beehive

go 1.13

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/K-Phoen/grabana v0.4.1
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/coreos/etcd v3.3.17+incompatible
	github.com/deepfabric/c-nemo v0.0.0-20200217131733-d8d20abdb6f7 // indirect
	github.com/deepfabric/go-nemo v0.0.0-20200217132256-1a30b09e0871
	github.com/deepfabric/prophet v0.0.0-20200605082442-0c0aa24123f6
	github.com/dgraph-io/badger v1.6.0
	github.com/fagongzi/goetty v1.3.2
	github.com/fagongzi/log v0.0.0-20191122063922-293b75312445
	github.com/fagongzi/util v0.0.0-20191031020235-c0f29a56724d
	github.com/funny/slab v0.0.0-20180511031532-b1fad5e5d478 // indirect
	github.com/funny/utest v0.0.0-20161029064919-43870a374500 // indirect
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/gogo/protobuf v1.3.1
	github.com/google/btree v1.0.0
	github.com/prometheus/client_golang v1.4.1
	github.com/shirou/gopsutil v2.19.9+incompatible
	github.com/stretchr/testify v1.4.0
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
)

replace github.com/coreos/etcd => github.com/deepfabric/etcd v3.3.17+incompatible
