module github.com/matrixorigin/matrixcube

go 1.16

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/K-Phoen/grabana v0.4.1
	github.com/RoaringBitmap/roaring v0.9.4
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/cockroachdb/errors v1.8.2
	github.com/cockroachdb/pebble v0.0.0-20210503173641-1387689d3d7c
	github.com/coreos/go-semver v0.3.0
	github.com/docker/go-units v0.4.0
	github.com/fagongzi/goetty v1.8.0
	github.com/fagongzi/log v0.0.0-20191122063922-293b75312445
	github.com/fagongzi/util v0.0.0-20210409031311-a10fdf8fbd7a
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/gogo/protobuf v1.3.2
	github.com/google/btree v1.0.1
	github.com/juju/ratelimit v1.0.1
	github.com/lni/vfs v0.2.1-0.20210810090357-27c7525cf64f
	github.com/montanaflynn/stats v0.6.6
	github.com/phf/go-queue v0.0.0-20170504031614-9abe38d0371d
	github.com/prometheus/client_golang v1.11.0
	github.com/shirou/gopsutil v3.21.7+incompatible
	github.com/stretchr/testify v1.7.0
	github.com/tklauser/go-sysconf v0.3.7 // indirect
	go.etcd.io/etcd/api/v3 v3.5.0
	go.etcd.io/etcd/client/pkg/v3 v3.5.0
	go.etcd.io/etcd/client/v3 v3.5.0
	go.etcd.io/etcd/raft/v3 v3.5.0
	go.etcd.io/etcd/server/v3 v3.5.0
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
)

replace go.etcd.io/etcd/raft/v3 => github.com/matrixorigin/etcd/raft/v3 v3.5.1-0.20210824022435-0203115049c2

replace go.etcd.io/etcd/v3 => github.com/matrixorigin/etcd/v3 v3.5.1-0.20210824022435-0203115049c2
