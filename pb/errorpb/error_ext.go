// Copyright 2021 MatrixOrigin.
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

package errorpb

// HasError returns true if is not a empty error
func HasError(err Error) bool {
	return err.Message != ""
}

// Retryable return true meas a retryable error
func Retryable(err Error) bool {
	return HasError(err) &&
		err.RaftEntryTooLarge == nil && // can not retry
		err.ShardUnavailable == nil &&
		err.LeaseMismatch == nil
}
