module github.com/deepfabric/prophet

go 1.15

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/coreos/go-semver v0.2.0
	github.com/docker/go-units v0.4.0
	github.com/fagongzi/goetty v2.0.1+incompatible
	github.com/fagongzi/util v0.0.0-20201116094402-221cc40c4593
	github.com/gogo/protobuf v1.3.1
	github.com/google/btree v1.0.0
	github.com/juju/ratelimit v1.0.1
	github.com/montanaflynn/stats v0.6.4
	github.com/phf/go-queue v0.0.0-20170504031614-9abe38d0371d
	github.com/prometheus/client_golang v1.0.0
	github.com/stretchr/testify v1.7.0
	go.etcd.io/etcd v0.0.0-20201125193152-8a03d2e9614b
)

replace go.etcd.io/etcd => github.com/deepfabric/etcd v0.0.0-20201207015257-3b4a2ca4cf64
