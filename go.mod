module github.com/deepfabric/beehive

go 1.13

require (
	github.com/K-Phoen/grabana v0.4.1
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/cockroachdb/pebble v0.0.0-20201125222200-e6d62a79c8e3
	github.com/deepfabric/prophet v0.1.1-0.20210402054224-6967f3c670d7
	github.com/fagongzi/goetty v2.0.2+incompatible
	github.com/fagongzi/log v0.0.0-20191122063922-293b75312445
	github.com/fagongzi/util v0.0.0-20210409031311-a10fdf8fbd7a
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/gogo/protobuf v1.3.1
	github.com/google/btree v1.0.0
	github.com/prometheus/client_golang v1.4.1
	github.com/shirou/gopsutil v2.19.9+incompatible
	github.com/stretchr/testify v1.7.0
	go.etcd.io/etcd v0.0.0-20201125193152-8a03d2e9614b
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
)

replace go.etcd.io/etcd => github.com/deepfabric/etcd v3.4.15+incompatible
