# Copyright 2021 MatrixOrigin.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

# supported environmental variables
#
# MEMFS_TEST=1 
# when MEMFS_TEST is set, some tests will try to use an in memory fs
# for testing. this will also enable the file descriptor leak check. 
# note that MEMFS_TEST doesn't affect production systems.
#
# GOROUTINE_LEAK_CHECK=1
# when GOROUTINE_LEAK_CHECK is set, some tests will try to use a goroutine
# leak check method to identify leaked goroutines. note that 
# GOROUTINE_LEAK_CHECK doesn't affect production systems. 

GOEXEC ?= go
PKGNAME = $(shell go list)
ROOT_DIR = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))/

ifeq ($(VERBOSE),)
	SILENT_MARK := @
else
	SILENT_MARK :=
endif

ifeq ($(NO_RACE),1)
RACE_FLAG=
$(warning "data race detector disabled")
else
RACE_FLAG=-race
endif

ifeq ($(COVER),1)
COVER_FLAG=-coverprofile=coverage.out
$(warning "coverage enabled, `go tool cover -html=coverage.out` to visualize")
endif

ifneq ($(TEST_TO_RUN),)
$(info Running selected tests $(TEST_TO_RUN))
SELECTED_TESTS=-run $(TEST_TO_RUN)
endif

ifeq ($(shell uname), Darwin)
### For issue: https://github.com/matrixorigin/matrixcube/issues/720
	TEST_ENV=CGO_ENABLED=1
else
	TEST_ENV=
endif
TEST_TAGS=-tags matrixone_test
SHORT_ONLY=-short
TEST_OPTIONS=test -timeout=3600s -count=1 -v $(RACE_FLAG) $(COVER_FLAG) $(SELECTED_TESTS)
GOTEST = $(SILENT_MARK)$(TEST_ENV) $(GOEXEC) $(TEST_OPTIONS) $(SHORT_ONLY) $(TEST_TAGS)

###############################################################################
# tests
###############################################################################

.PHONY: all
all: test

.PHONY: test-util
test-util:
	$(GOTEST) $(PKGNAME)/util
	$(GOTEST) $(PKGNAME)/util/hlc
	$(GOTEST) $(PKGNAME)/util/keys
	$(GOTEST) $(PKGNAME)/util/buf
	$(GOTEST) $(PKGNAME)/util/fileutil
	$(GOTEST) $(PKGNAME)/util/stop
	$(GOTEST) $(PKGNAME)/util/task

.PHONY: test-pb
test-pb:
	$(GOTEST) $(PKGNAME)/pb/txnpb
	$(GOTEST) $(PKGNAME)/pb/hlcpb

.PHONY: test-storage
test-storage:
	$(GOTEST) $(PKGNAME)/storage/kv
	$(GOTEST) $(PKGNAME)/storage/executor

.PHONY: test-snapshot
test-snapshot:
	$(GOTEST) $(PKGNAME)/snapshot

.PHONY: test-logdb
test-logdb:
	$(GOTEST) $(PKGNAME)/logdb

.PHONY: test-raftstore
test-raftstore:
	$(GOTEST) $(PKGNAME)/raftstore

.PHONY: test-all-raftstore
test-all-raftstore: override SHORT_ONLY :=
test-all-raftstore: test-raftstore

.PHONY: test-client
test-client:
	$(GOTEST) $(PKGNAME)/client

.PHONY: test-transport
test-transport:
	$(GOTEST) $(PKGNAME)/transport

.PHONY: test-keys
test-keys:
	$(GOTEST) $(PKGNAME)/keys

.PHONY: test-prophet
test-prophet:
	$(GOTEST) $(PKGNAME)/components/prophet
	$(GOTEST) $(PKGNAME)/components/prophet/id
	$(GOTEST) $(PKGNAME)/components/prophet/storage
	$(GOTEST) $(PKGNAME)/components/prophet/core

.PHONY: test-txn
test-txn:
	$(GOTEST) $(PKGNAME)/txn
	$(GOTEST) $(PKGNAME)/txn/client
	$(GOTEST) $(PKGNAME)/txn/kv
	$(GOTEST) $(PKGNAME)/txn/util

.PHONY: integration-test
integration-test: test-all-raftstore

.PHONY: components-unit-test
components-unit-test: test-storage test-logdb test-client test-transport test-keys \
  test-snapshot test-prophet test-txn test-pb test-util

.PHONY: test
test: components-unit-test test-raftstore

.PHONY: all-tests
all-tests: components-unit-test test-all-raftstore

.PHONY: pb
pb:
	cd $(ROOT_DIR)/pb; ./gen.sh; cd $(ROOT_DIR)/pb; ./gen.sh

.PHONY: fpm
fpm:
	rm -rf /usr/local/bin/fpm; go build -o /usr/local/bin/fpm $(ROOT_DIR)/util/fpm/*.go

###############################################################################
# static checks
###############################################################################

.PHONY: install-static-check-tools
install-static-check-tools:
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | bash -s -- -b $GOROOT/bin v1.45.2

CHECKED_PKGS=$(shell go list ./... | grep -v prophet)
CHECKED_DIRS=$(subst $(PKGNAME), ,$(subst $(PKGNAME)/, ,$(CHECKED_PKGS))) .
EXTRA_LINTERS=-E misspell -E exportloopref -E rowserrcheck -E depguard -E unconvert \
	-E gofmt -E stylecheck -E predeclared -E nilnil 

.PHONY: static-check
static-check:
	@for p in $(CHECKED_DIRS); do \
		golangci-lint run $(EXTRA_LINTERS) $$p; \
  done;
