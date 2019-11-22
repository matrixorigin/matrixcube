ROOT_DIR = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))/
LD_FLAGS = -ldflags "-w -s"

GOOS 		= linux
DIST_DIR 	= $(ROOT_DIR)dist/

.PHONY: dist_dir
dist_dir: ; $(info ======== prepare distribute dir:)
	mkdir -p $(DIST_DIR)
	@rm -rf $(DIST_DIR)*

.PHONY: redis
redis: dist_dir; $(info ======== compiled beehive example redis:)
	env GO111MODULE=off GOOS=$(GOOS) go build -o $(DIST_DIR)redis $(LD_FLAGS) $(ROOT_DIR)example/redis/*.go

.PHONY: example
example: ; $(info ======== compiled beehive example:)
	docker build -t deepfabric/beehive-redis .