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
// Queue
////////////////////////////////////////////////////////////////////////////////

// Queue provides the functionality of a queue - specifically, a
// FIFO (first-in, first-out) data structure.
type Queue[T any] struct {
	tree tree[omit, T]
}

// QueueOptions are options for NewQueueOptoins
type QueueOptions[T any] struct {
	Copy    func(T) T
	Release func(T)
}

// NewQueueOptions returns a new queue
func NewQueueOptions[T any](opts QueueOptions[T]) *Queue[T] {
	b := new(Queue[T])
	b.tree.dataCopy = opts.Copy
	b.tree.dataRelease = opts.Release
	return b
}

// NewQueue returns a new queue
func NewQueue[T any]() *Queue[T] {
	return NewQueueOptions(QueueOptions[T]{})
}

// Push an item to end of queue.
func (b *Queue[T]) Push(item T) {
	b.tree.PushBack(omit{}, item)
}

// Pop first item from queue.
func (b *Queue[T]) Pop() (T, bool) {
	_, item, ok := b.tree.PopFront()
	return item, ok
}

// PopFunc deletes first item if provided function allows.
func (b *Queue[T]) PopFunc(cond func(item T) bool) (T, bool) {
	if cond == nil {
		return b.Pop()
	}
	_, item, ok := b.tree.PopFrontFunc(func(_ omit, item T) bool {
		return cond(item)
	})
	return item, ok
}

// Front returns the first item in queue, or false if queue is empty.
func (b *Queue[T]) Front() (T, bool) {
	_, item, ok := b.tree.Front()
	return item, ok
}

// FrontMut returns the first item in queue, or false if queue is empty.
// MUTABLE OPERATION.
func (b *Queue[T]) FrontMut() (T, bool) {
	_, item, ok := b.tree.FrontMut()
	return item, ok
}

// At returns the item At position, after first, in queue.
// Returns false if no item is found At position.
func (b *Queue[T]) At(pos int) (T, bool) {
	_, item, ok := b.tree.LookupAt(pos)
	return item, ok
}

// AtMut returns the item at position, after first, in queue.
// Returns false if no item is found at position.
// MUTABLE OPERATION.
func (b *Queue[T]) AtMut(pos int) (T, bool) {
	_, item, ok := b.tree.LookupAtMut(pos)
	return item, ok
}

// Len returns the number of items in queue
func (b *Queue[T]) Len() int {
	return b.tree.Len()
}

// Clone the queue.
// This is a fast O(1) operation using a copy-on-write method.
func (b *Queue[T]) Clone() *Queue[T] {
	b2 := new(Queue[T])
	*b2 = *b
	b.tree.CloneInto(&b2.tree)
	return b2
}

// Clear the queue.
func (b *Queue[T]) Clear() {
	b.tree.Clear()
}

// Release clears the queue and release any references.
// This method is functionally equivalent to Clear() but is an optimization for
// collections that are copied using Clone().
func (b *Queue[T]) Release() {
	b.tree.Release()
}

// All returns an iterator of all items starting with the first.
func (b *Queue[T]) All() iter.Seq[T] {
	return iterV(b.tree.All())
}

// All returns an iterator of all items starting with the first.
// MUTABLE OPERATION.
func (b *Queue[T]) AllMut() iter.Seq[T] {
	return iterV(b.tree.AllMut())
}

// Drain removes all items from queue and returns them as an iterator.
func (b *Queue[T]) Drain() iter.Seq[T] {
	return iterV(b.tree.Drain())
}

// Contains returns true if item exists.
func (b *Queue[T]) Contains(item T) bool {
	cmp := CompareFor[T]()
	for vitem := range b.All() {
		if cmp(vitem, item) == 0 {
			return true
		}
	}
	return false
}

// String representation of queue
func (b *Queue[T]) String() string {
	return fmt.Sprintf("queue%v", slices.Collect(b.All()))
}
