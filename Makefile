ROOT_DIR = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))/
LD_FLAGS = -ldflags "-w -s"

GOOS 		= linux
DIST_DIR 	= $(ROOT_DIR)dist/

.PHONY: dist_dir
dist_dir: ; $(info ======== prepare distribute dir:)
	mkdir -p $(DIST_DIR)
	#@rm -rf $(DIST_DIR)*

.PHONY: redis
redis: dist_dir; $(info ======== compiled beehive example redis:)
	env GO111MODULE=off GOOS=$(GOOS) go build -o $(DIST_DIR)redis $(LD_FLAGS) $(ROOT_DIR)example/redis/*.go

.PHONY: http
http: dist_dir; $(info ======== compiled beehive example http:)
	env CGO_ENABLED=0 GOOS=$(GOOS) go build -mod vendor -a -installsuffix cgo -o $(DIST_DIR)http $(LD_FLAGS) $(ROOT_DIR)example/http/*.go

.PHONY: example-redis
example-redis: ; $(info ======== compiled beehive redis example:)
	docker build -t deepfabric/beehive-redis -f Dockerfile-redis .

.PHONY: example-http
example-http: http; $(info ======== compiled beehive http example:)
	docker build -t deepfabric/beehive-http -f Dockerfile-http .