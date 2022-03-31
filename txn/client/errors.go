// Copyright 2022 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import "errors"

var (
	// ErrTxnEnding no operations can be performed during ending transaction
	ErrTxnEnding = errors.New("no operations can be performed during ending transaction")
	// ErrTxnCommitted no operation can be executed after the transaction is committed
	ErrTxnCommitted = errors.New("no operation can be executed after the transaction is committed")
	// ErrTxnAborted no operation can be executed after the transaction is aborted
	ErrTxnAborted = errors.New("no operation can be executed after the transaction is aborted")
	// ErrTxnConflict transaction operations encounter conflicts and the client needs to rollback
	// the transaction
	ErrTxnConflict = errors.New("transaction operations encounter conflicts")
	// ErrTxnUncertainty uncertain clock error encountered
	ErrTxnUncertainty = errors.New("uncertain clock error encountered")
)
