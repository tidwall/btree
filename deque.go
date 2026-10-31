// https://github.com/tidwall/btree
//
// Copyright 2026 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
//
// B-tree based collections for go
package btree // import "github.com/tidwall/btree/v2"

import (
	"fmt"
	"iter"
	"slices"
)

////////////////////////////////////////////////////////////////////////////////
// Deque
////////////////////////////////////////////////////////////////////////////////

// Deque is a double-ended queue
type Deque[T any] struct {
	tree tree[omit, T]
}

// DequeOptions are options for deque collection
type DequeOptions[T any] struct {
	Copy    func(T) T // Optional copy item function
	Release func(T)   // Optional release item function
}

// NewDequeOptions returns a new deque
func NewDequeOptions[T any](opts DequeOptions[T]) *Deque[T] {
	b := new(Deque[T])
	b.tree.dataCopy = opts.Copy
	b.tree.dataRelease = opts.Release
	return b
}

// NewDeque returns a new deque
func NewDeque[T any]() *Deque[T] {
	return NewDequeOptions(DequeOptions[T]{})
}

// PushFront inserts item to front of queue
func (b *Deque[T]) PushFront(item T) {
	b.tree.PushFront(omit{}, item)
}

// PushFront inserts item to back of queue
func (b *Deque[T]) PushBack(item T) {
	b.tree.PushBack(omit{}, item)
}

// PopFront removes first item in queue
func (b *Deque[T]) PopFront() (T, bool) {
	_, val, ok := b.tree.PopFront()
	return val, ok
}

// PopFront removes last item in queue
func (b *Deque[T]) PopBack() (T, bool) {
	_, val, ok := b.tree.PopBack()
	return val, ok
}

// PopFrontFunc removes first item if provided function allows.
func (b *Deque[T]) PopFrontFunc(cond func(item T) bool) (T, bool) {
	if cond == nil {
		return b.PopFront()
	}
	_, value, ok := b.tree.PopFrontFunc(func(_ omit, item T) bool {
		return cond(item)
	})
	return value, ok
}

// PopBackFunc removes last item if provided function allows.
func (b *Deque[T]) PopBackFunc(cond func(item T) bool) (T, bool) {
	if cond == nil {
		return b.PopBack()
	}
	_, value, ok := b.tree.PopBackFunc(func(_ omit, item T) bool {
		return cond(item)
	})
	return value, ok
}

// Front returns first item
func (b *Deque[T]) Front() (T, bool) {
	_, item, ok := b.tree.Front()
	return item, ok
}

// Front returns first item
// MUTABLE OPERATION
func (b *Deque[T]) FrontMut() (T, bool) {
	_, item, ok := b.tree.FrontMut()
	return item, ok
}

// Back returns last item
func (b *Deque[T]) Back() (T, bool) {
	_, item, ok := b.tree.Back()
	return item, ok
}

// BackMut returns last item
// MUTABLE OPERATION
func (b *Deque[T]) BackMut() (T, bool) {
	_, item, ok := b.tree.BackMut()
	return item, ok
}

// At returns the item At position, after first, in queue.
// Returns false if no item is found At position.
func (b *Deque[T]) At(pos int) (T, bool) {
	_, item, ok := b.tree.LookupAt(pos)
	return item, ok
}

// AtMut returns the item at position, after first, in queue.
// Returns false if no item is found at position.
// MUTABLE OPERATION.
func (b *Deque[T]) AtMut(pos int) (T, bool) {
	_, item, ok := b.tree.LookupAtMut(pos)
	return item, ok
}

// Len returns the number of items in queue
func (b *Deque[T]) Len() int {
	return b.tree.Len()
}

// Clone the queue.
// This is a fast O(1) operation using a copy-on-write method.
func (b *Deque[T]) Clone() *Deque[T] {
	b2 := new(Deque[T])
	*b2 = *b
	b.tree.CloneInto(&b2.tree)
	return b2
}

// Clear the queue.
func (b *Deque[T]) Clear() {
	b.tree.Clear()
}

// Release clears the queue and release any references.
// This method is functionally equivalent to Clear() but is an optimization for
// collections that are copied using Copy().
func (b *Deque[T]) Release() {
	b.tree.Release()
}

// All returns an iterator of all items starting with the first.
func (b *Deque[T]) All() iter.Seq[T] {
	return iterV(b.tree.All())
}

// All returns an iterator of all items starting with the first.
// MUTABLE OPERATION.
func (b *Deque[T]) AllMut() iter.Seq[T] {
	return iterV(b.tree.AllMut())
}

// Backward returns an iterator of all items starting with the last.
func (b *Deque[T]) Backward() iter.Seq[T] {
	return iterV(b.tree.Backward())
}

// BackwardMut returns an iterator of all items starting with the last.
// MUTABLE OPERATION.
func (b *Deque[T]) BackwardMut() iter.Seq[T] {
	return iterV(b.tree.BackwardMut())
}

// Drain removes all items and returns them as an iterator starting with the
// first.
func (b *Deque[T]) Drain() iter.Seq[T] {
	return iterV(b.tree.Drain())
}

// DrainBackward removes all items and returns them as an iterator starting
// with the last.
func (b *Deque[T]) DrainBackward() iter.Seq[T] {
	return iterV(b.tree.DrainBackward())
}

// Contains returns true if queue contains item.
func (b *Deque[T]) Contains(item T) bool {
	cmp := CompareFor[T]()
	for vitem := range b.All() {
		if cmp(vitem, item) == 0 {
			return true
		}
	}
	return false
}

// String returns a string representation of queue.
func (b *Deque[T]) String() string {
	return fmt.Sprintf("deque%v\n", slices.Collect(b.All()))
}
