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

ifeq ($(RACE),1)
RACE_FLAG=-race
$(warning "data race detector enabled")
endif

ifeq ($(COVER),1)
COVER_FLAG=-coverprofile=coverage.out
$(warning "coverage enabled, `go tool cover -html=coverage.out` to visualize")
endif

ifneq ($(TEST_TO_RUN),)
$(info Running selected tests $(TEST_TO_RUN))
SELECTED_TESTS=-run $(TEST_TO_RUN)
endif

TEST_OPTIONS=test -timeout=300s -count=1 -v $(RACE_GLAG) $(COVER_FLAG) $(SELECTED_TESTS)
GOTEST=$(GO) $(TEST_OPTIONS)

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

.PHONY: test-server
test-server:
	$(GOTEST) $(PKGNAME)/server

.PHONY: test-transport
test-transport:
	$(GOTEST) $(PKGNAME)/transport

.PHONY: test-keys
test-keys:
	$(GOTEST) $(PKGNAME)/keys

.PHONY: test
test: test-storage test-logdb test-raftstore test-server test-transport \
	test-keys test-snapshot

###############################################################################
# static checks
###############################################################################

# TODO: switch to the following two lists after some major cleanups
# TODO: switch to a more recent version of golangci-lint, currently on v1.23.8
# PKGS=$(shell go list ./...)
# DIRS=$(subst $(PKGNAME), ,$(subst $(PKGNAME)/, ,$(CHECKED_PKGS))) .
PKGS=$(PKGNAME)/storage $(PKGNAME)/storage/kv $(PKGNAME)/storage/executor/simple \
	$(PKGNAME)/storage/stats $(PKGNAME)/snapshot
DIRS=storage storage/kv storage/executor/simple storage/stats snapshot
EXTRA_LINTERS=-E misspell -E scopelint -E rowserrcheck -E depguard -E unconvert \
  -E prealloc -E gofmt -E stylecheck

.PHONY: static-check
static-check:
	@for p in $(PKGS); do \
    go vet -tests=false $$p; \
    golint $$p; \
    ineffassign $$p; \
		errcheck -blank -ignoretests $$p; \
  done;
	@for p in $(DIRS); do \
    golangci-lint run $(EXTRA_LINTERS) $$p; \
  done;
