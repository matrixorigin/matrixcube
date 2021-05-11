package deque

// Deque deque
type Deque interface {
	// Len returns the number of elements of Deque.
	// The complexity is O(1).
	Len() int
	// Clear clears Deque
	Clear()
	// ForEach call fn on all elements which after the offset, stopped if false returned
	ForEach(offset int, fn func(interface{}) bool)
	// Front returns the first element of Deque, false if the list is empty.
	Front() (*Element, bool)
	// PopFront removes and returns the first element of Deque
	PopFront() *Element
	// Front returns the first element of Deque, panic if the list is empty.
	MustFront() *Element
	// Back returns the last element of Deque, false if the list is empty.
	Back() (*Element, bool)
	// PopBack removes and returns the last element of Deque
	PopBack() *Element
	// MustBack returns the last element of Deque, panic if the list is empty.
	MustBack() *Element
	// PushFront inserts a new element e with value v at the front of deque.
	PushFront(v interface{}) *Element
	// PushBack inserts a new element e with value v at the back of deque.
	PushBack(v interface{}) *Element
	// InsertBefore inserts a new element e with value v immediately before mark and returns e.
	// If mark is not an element of l, the list is not modified.
	// The mark must not be nil.
	InsertBefore(v interface{}, mark *Element) *Element
	// InsertAfter inserts a new element e with value v immediately after mark and returns e.
	// If mark is not an element of l, the list is not modified.
	// The mark must not be nil.
	InsertAfter(v interface{}, mark *Element) *Element
	// MoveToFront moves element e to the front of list l.
	// If e is not an element of l, the list is not modified.
	// The element must not be nil.
	MoveToFront(e *Element)
	// MoveToBack moves element e to the back of list l.
	// If e is not an element of l, the list is not modified.
	// The element must not be nil.
	MoveToBack(e *Element)
	// MoveBefore moves element e to its new position before mark.
	// If e or mark is not an element of l, or e == mark, the list is not modified.
	// The element and mark must not be nil.
	MoveBefore(e, mark *Element)
	// MoveAfter moves element e to its new position after mark.
	// If e or mark is not an element of l, or e == mark, the list is not modified.
	// The element and mark must not be nil.
	MoveAfter(e, mark *Element)
	// Remove removes e from l if e is an element of list l.
	// It returns the element value e.Value.
	// The element must not be nil.
	Remove(e *Element) interface{}
	// Truncate trancate deque, keeping the first size elements
	Truncate(keeping int)
	// Drain removes the specified range in the deque, returns drained
	Drain(from, to int) Deque
}

// New returns an initialized Deque.
func New() Deque {
	q := new(defaultDeque)
	q.Clear()
	return q
}

// Element is an Element of a linked Deque.
type Element struct {
	// Next and previous pointers in the doubly-linked Deque of elements.
	// To simplify the implementation, internally a Deque l is implemented
	// as a ring, such that &l.root is both the next element of the last
	// Deque element (l.Back()) and the previous element of the first Deque
	// element (l.Front()).
	next, prev *Element

	// The list to which this element belongs.
	list *defaultDeque

	// The value stored with this element.
	Value interface{}
}

// Next returns the next Deque element or nil.
func (e *Element) Next() *Element {
	if p := e.next; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

// Prev returns the previous Deque element or nil.
func (e *Element) Prev() *Element {
	if p := e.prev; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

type defaultDeque struct {
	root Element // sentinel list element, only &root, root.prev, and root.next are used
	len  int     // current list length excluding (this) sentinel element
}

func (q *defaultDeque) Clear() {
	q.root.next = &q.root
	q.root.prev = &q.root
	q.len = 0
}

func (q *defaultDeque) Truncate(keeping int) {
	if keeping >= q.len {
		return
	}

	q.doRangeRemove(keeping, q.len, false)
}

func (q *defaultDeque) Len() int { return q.len }

func (q *defaultDeque) ForEach(offset int, fn func(interface{}) bool) {
	if q.len == 0 {
		return
	}

	skipped := 0
	v, _ := q.Front()
	for e := v; e != nil; e = e.Next() {
		if skipped < offset {
			skipped++
			continue
		}

		if !fn(e.Value) {
			return
		}
	}
}

func (q *defaultDeque) Front() (*Element, bool) {
	if q.len == 0 {
		return nil, false
	}
	return q.root.next, true
}

func (q *defaultDeque) PopFront() *Element {
	if q.len == 0 {
		return nil
	}

	return q.remove(q.root.next)
}

func (q *defaultDeque) MustFront() *Element {
	if q.len == 0 {
		panic("MustFront on a empty deque")
	}

	return q.root.next
}

func (q *defaultDeque) Back() (*Element, bool) {
	if q.len == 0 {
		return nil, false
	}
	return q.root.prev, true
}

func (q *defaultDeque) PopBack() *Element {
	if q.len == 0 {
		return nil
	}

	return q.remove(q.root.prev)
}

func (q *defaultDeque) MustBack() *Element {
	if q.len == 0 {
		panic("MustBack on a empty deque")
	}

	return q.root.prev
}

func (q *defaultDeque) PushFront(v interface{}) *Element {
	q.lazyInit()
	return q.insertValue(v, &q.root)
}

func (q *defaultDeque) PushBack(v interface{}) *Element {
	q.lazyInit()
	return q.insertValue(v, q.root.prev)
}

func (q *defaultDeque) Drain(from, to int) Deque {
	return q.doRangeRemove(from, to, true)
}

// lazyInit lazily initializes a zero List value.
func (q *defaultDeque) lazyInit() {
	if q.root.next == nil {
		q.Clear()
	}
}

// insert inserts e after at, increments l.len, and returns e.
func (q *defaultDeque) insert(e, at *Element) *Element {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
	e.list = q
	q.len++
	return e
}

// insertValue is a convenience wrapper for insert(&Element{Value: v}, at).
func (q *defaultDeque) insertValue(v interface{}, at *Element) *Element {
	return q.insert(&Element{Value: v}, at)
}

// remove removes e from its list, decrements l.len, and returns e.
func (q *defaultDeque) remove(e *Element) *Element {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil // avoid memory leaks
	e.prev = nil // avoid memory leaks
	e.list = nil
	q.len--
	return e
}

// move moves e to next to at and returns e.
func (q *defaultDeque) move(e, at *Element) *Element {
	if e == at {
		return e
	}
	e.prev.next = e.next
	e.next.prev = e.prev

	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e

	return e
}

func (q *defaultDeque) Remove(e *Element) interface{} {
	if e.list == q {
		// if e.list == l, l must have been initialized when e was inserted
		// in l or l == nil (e is a zero Element) and l.remove will crash
		q.remove(e)
	}
	return e.Value
}

func (q *defaultDeque) InsertBefore(v interface{}, mark *Element) *Element {
	if mark.list != q {
		return nil
	}
	// see comment in List.Remove about initialization of l
	return q.insertValue(v, mark.prev)
}

func (q *defaultDeque) InsertAfter(v interface{}, mark *Element) *Element {
	if mark.list != q {
		return nil
	}
	// see comment in List.Remove about initialization of l
	return q.insertValue(v, mark)
}

func (q *defaultDeque) MoveToFront(e *Element) {
	if e.list != q || q.root.next == e {
		return
	}
	// see comment in List.Remove about initialization of l
	q.move(e, &q.root)
}

func (q *defaultDeque) MoveToBack(e *Element) {
	if e.list != q || q.root.prev == e {
		return
	}
	// see comment in List.Remove about initialization of l
	q.move(e, q.root.prev)
}

func (q *defaultDeque) MoveBefore(e, mark *Element) {
	if e.list != q || e == mark || mark.list != q {
		return
	}
	q.move(e, mark.prev)
}

func (q *defaultDeque) MoveAfter(e, mark *Element) {
	if e.list != q || e == mark || mark.list != q {
		return
	}
	q.move(e, mark)
}

func (q *defaultDeque) doRangeRemove(from, to int, withRemoved bool) Deque {
	if from >= to {
		return nil
	}

	q.lazyInit()
	if q.len == 0 {
		return nil
	}

	if to > q.len {
		to = q.len
	}

	i := 0
	var left *Element
	var drainedRight *Element
	right := &q.root
	for e := q.root.next; e != &q.root && e.list != nil; e = e.next {
		if i >= from && i < to {
			if left == nil {
				left = e
			}
			drainedRight = e
		} else if i >= to {
			right = e
			break
		}

		i++
	}

	q.len -= i - from
	left.prev.next = right
	right.prev = left.prev
	if right == &q.root {
		q.root.prev = left.prev
	}

	if !withRemoved {
		return nil
	}

	drained := new(defaultDeque)
	drained.Clear()
	left.prev = &drained.root
	drained.root.next = left
	drained.root.prev = drainedRight
	drainedRight.next = &drained.root
	drained.len = i - from
	for e := left; e != &q.root && e.list != nil; e = e.next {
		e.list = drained
		if e == drainedRight {
			break
		}
	}
	return drained
}
