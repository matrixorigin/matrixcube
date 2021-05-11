// Copyright (c) 2013-2017, Peter H. Froehlich. All rights reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file.

// Package queue implements a double-ended queue (aka "deque") data structure
// on top of a slice. All operations run in (amortized) constant time.
// Benchmarks compare favorably to container/list as well as to Go's channels.
// These queues are not safe for concurrent use.
package queue

import (
	"bytes"
	"fmt"
)

// Queue represents a double-ended queue.
// The zero value is an empty queue ready to use.
type Queue struct {
	// PushBack writes to rep[back] then increments back; PushFront
	// decrements front then writes to rep[front]; len(rep) is a power
	// of two; unused slots are nil and not garbage.
	rep    []interface{}
	front  int
	back   int
	length int
}

// New returns an initialized empty queue.
func New() *Queue {
	return new(Queue).Init()
}

// Init initializes or clears queue q.
func (q *Queue) Init() *Queue {
	q.rep = make([]interface{}, 1)
	q.front, q.back, q.length = 0, 0, 0
	return q
}

// lazyInit lazily initializes a zero Queue value.
//
// I am mostly doing this because container/list does the same thing.
// Personally I think it's a little wasteful because every single
// PushFront/PushBack is going to pay the overhead of calling this.
// But that's the price for making zero values useful immediately.
func (q *Queue) lazyInit() {
	if q.rep == nil {
		q.Init()
	}
}

// Len returns the number of elements of queue q.
func (q *Queue) Len() int {
	return q.length
}

// empty returns true if the queue q has no elements.
func (q *Queue) empty() bool {
	return q.length == 0
}

// full returns true if the queue q is at capacity.
func (q *Queue) full() bool {
	return q.length == len(q.rep)
}

// sparse returns true if the queue q has excess capacity.
func (q *Queue) sparse() bool {
	return 1 < q.length && q.length < len(q.rep)/4
}

// resize adjusts the size of queue q's underlying slice.
func (q *Queue) resize(size int) {
	adjusted := make([]interface{}, size)
	if q.front < q.back {
		// rep not "wrapped" around, one copy suffices
		copy(adjusted, q.rep[q.front:q.back])
	} else {
		// rep is "wrapped" around, need two copies
		n := copy(adjusted, q.rep[q.front:])
		copy(adjusted[n:], q.rep[:q.back])
	}
	q.rep = adjusted
	q.front = 0
	q.back = q.length
}

// lazyGrow grows the underlying slice if necessary.
func (q *Queue) lazyGrow() {
	if q.full() {
		q.resize(len(q.rep) * 2)
	}
}

// lazyShrink shrinks the underlying slice if advisable.
func (q *Queue) lazyShrink() {
	if q.sparse() {
		q.resize(len(q.rep) / 2)
	}
}

// String returns a string representation of queue q formatted
// from front to back.
func (q *Queue) String() string {
	var result bytes.Buffer
	result.WriteByte('[')
	j := q.front
	for i := 0; i < q.length; i++ {
		result.WriteString(fmt.Sprintf("%v", q.rep[j]))
		if i < q.length-1 {
			result.WriteByte(' ')
		}
		j = q.inc(j)
	}
	result.WriteByte(']')
	return result.String()
}

// inc returns the next integer position wrapping around queue q.
func (q *Queue) inc(i int) int {
	return (i + 1) & (len(q.rep) - 1) // requires l = 2^n
}

// dec returns the previous integer position wrapping around queue q.
func (q *Queue) dec(i int) int {
	return (i - 1) & (len(q.rep) - 1) // requires l = 2^n
}

// Front returns the first element of queue q or nil.
func (q *Queue) Front() interface{} {
	// no need to check q.empty(), unused slots are nil
	return q.rep[q.front]
}

// Back returns the last element of queue q or nil.
func (q *Queue) Back() interface{} {
	// no need to check q.empty(), unused slots are nil
	return q.rep[q.dec(q.back)]
}

// PushFront inserts a new value v at the front of queue q.
func (q *Queue) PushFront(v interface{}) {
	q.lazyInit()
	q.lazyGrow()
	q.front = q.dec(q.front)
	q.rep[q.front] = v
	q.length++
}

// PushBack inserts a new value v at the back of queue q.
func (q *Queue) PushBack(v interface{}) {
	q.lazyInit()
	q.lazyGrow()
	q.rep[q.back] = v
	q.back = q.inc(q.back)
	q.length++
}

// PopFront removes and returns the first element of queue q or nil.
func (q *Queue) PopFront() interface{} {
	if q.empty() {
		return nil
	}
	v := q.rep[q.front]
	q.rep[q.front] = nil // unused slots must be nil
	q.front = q.inc(q.front)
	q.length--
	q.lazyShrink()
	return v
}

// PopBack removes and returns the last element of queue q or nil.
func (q *Queue) PopBack() interface{} {
	if q.empty() {
		return nil
	}
	q.back = q.dec(q.back)
	v := q.rep[q.back]
	q.rep[q.back] = nil // unused slots must be nil
	q.length--
	q.lazyShrink()
	return v
}
