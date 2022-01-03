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
GO=@$(GOEXEC)
else
GO=$(GOEXEC)
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

TEST_TAGS=-tags matrixone_test
SHORT_ONLY=-short
TEST_OPTIONS=test -timeout=3600s -count=1 -v $(RACE_FLAG) $(COVER_FLAG) $(SELECTED_TESTS)
GOTEST=$(GO) $(TEST_OPTIONS) $(SHORT_ONLY) $(TEST_TAGS)

###############################################################################
# tests
###############################################################################

.PHONY: all
all: test

.PHONY: test-storage
test-storage:
	$(GOTEST) $(PKGNAME)/storage/kv
	$(GOTEST) $(PKGNAME)/storage/executor/simple

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

.PHONY: test-server
test-server:
	$(GOTEST) $(PKGNAME)/server

.PHONY: test-transport
test-transport:
	$(GOTEST) $(PKGNAME)/transport

.PHONY: test-keys
test-keys:
	$(GOTEST) $(PKGNAME)/keys

.PHONY: integration-test
integration-test: test-all-raftstore

.PHONY: components-unit-test
components-unit-test: test-storage test-logdb test-server test-transport test-keys \
  test-snapshot

.PHONY: test
test: components-unit-test test-raftstore

.PHONY: all-tests
all-tests: components-unit-test test-all-raftstore

.PHONY: pb
pb:
	cd $(ROOT_DIR)/pb; ./gen.sh; cd $(ROOT_DIR)/components/prophet/pb; ./gen.sh

###############################################################################
# static checks
###############################################################################

.PHONY: install-static-check-tools
install-static-check-tools:
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | bash -s -- -b $GOROOT/bin v1.43.0

# TODO: switch to the following two lists after some major cleanups
# TODO: switch to a more recent version of golangci-lint, currently on v1.23.8
# PKGS=$(shell go list ./...)
# DIRS=$(subst $(PKGNAME), ,$(subst $(PKGNAME)/, ,$(CHECKED_PKGS))) .
DIRS=storage \
	   storage/kv \
		 storage/executor/simple \
		 storage/stats \
		 config \
		 aware \
		 keys \
		 util \
		 vfs \
		 server \
		 snapshot \
		 transport \
		 logdb \
		 raftstore

EXTRA_LINTERS=-E misspell -E exportloopref -E rowserrcheck -E depguard -E unconvert \
	-E prealloc -E gofmt -E stylecheck

.PHONY: static-check
static-check:
	@for p in $(DIRS); do \
    golangci-lint run $(EXTRA_LINTERS) $$p; \
  done;
