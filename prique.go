// https://github.com/tidwall/btree
//
// Copyright 2026 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
//
// B-tree based collections for go
package btree // import "github.com/tidwall/btree/v2"

import (
	"cmp"
	"fmt"
	"iter"
	"slices"
)

////////////////////////////////////////////////////////////////////////////////
// Prique
////////////////////////////////////////////////////////////////////////////////

type pqitem[T any] struct {
	value T      `btree:"key"`
	seqid uint64 `btree:"key"`
}

// Prique is a priority queue
type Prique[T any] struct {
	table Table[pqitem[T]]
	seqid uint64
}

// PriqueOptions for NewPriqueOptions()
type PriqueOptions[T any] struct {
	Less    func(T, T) bool // Less comparator
	Compare func(T, T) int  // Standard comparator
	Copy    func(T) T       // Optional Copy function
	Release func(T)         // Optional Release function
}

// NewPriqueOptions returns a new priority queue
func NewPriqueOptions[T any](opts PriqueOptions[T]) *Prique[T] {
	b := new(Prique[T])
	var topts TableOptions[pqitem[T]]
	if opts.Compare != nil {
		topts.Compare = func(a, b pqitem[T]) int {
			c := opts.Compare(a.value, b.value)
			if c == 0 {
				c = cmp.Compare(a.seqid, b.seqid)
			}
			return c
		}
	}
	if opts.Less != nil {
		topts.Less = func(a, b pqitem[T]) bool {
			if opts.Less(a.value, b.value) {
				return true
			}
			if opts.Less(b.value, a.value) {
				return false
			}
			return a.seqid < b.seqid
		}
	}
	if opts.Copy != nil {
		topts.Copy = func(v pqitem[T]) pqitem[T] {
			v.value = opts.Copy(v.value)
			return v
		}
	}
	if opts.Release != nil {
		topts.Release = func(v pqitem[T]) {
			opts.Release(v.value)
		}
	}
	b.table = *NewTableOptions(topts)
	return b
}

// NewPrique returns a new priority queue
func NewPrique[T any]() *Prique[T] {
	return NewPriqueOptions(PriqueOptions[T]{})
}

// Push item into queue.
// Returns true if inserted or false if already exists
func (b *Prique[T]) Push(item T) bool {
	b.seqid++
	_, ok := b.table.Insert(pqitem[T]{item, b.seqid})
	return ok
}

// All returns an iterator of all items in queue
func (b *Prique[T]) All() iter.Seq[T] {
	return func(yield func(T) bool) {
		for v := range b.table.Backward() {
			if !yield(v.value) {
				break
			}
		}
	}
}

// AllMut returns an iterator of all items in queue
// MUTABLE OPERATION
func (b *Prique[T]) AllMut() iter.Seq[T] {
	return func(yield func(T) bool) {
		for v := range b.table.BackwardMut() {
			if !yield(v.value) {
				break
			}
		}
	}
}

// Drain iterates over each item in queue, popping along the way.
func (b *Prique[T]) Drain() iter.Seq[T] {
	return func(yield func(T) bool) {
		for v := range b.table.DrainBackward() {
			if !yield(v.value) {
				break
			}
		}
	}
}

// Delete item with the provided key.
// If duplicate items with the same key exist, only one will be deleted;
// specifically the oldest duplicate item is deleted.
func (b *Prique[T]) Delete(key T) (T, bool) {
	var empty T
	keyA := pqitem[T]{key, 0}
	v, ok := b.table.Seek(keyA)
	if ok {
		keyB := pqitem[T]{v.value, 0}
		c := b.table.tree.dataCompare(omit{}, keyA, omit{}, keyB)
		if c == 0 {
			v, ok = b.table.Delete(v)
			return v.value, ok
		}
	}
	return empty, false
}

// Pop next item from queue.
func (b *Prique[T]) Pop() (T, bool) {
	v, ok := b.table.PopBack()
	return v.value, ok
}

// PopFunc deletes next item if provided function allows.
func (b *Prique[T]) PopFunc(cond func(item T) bool) (T, bool) {
	if cond == nil {
		return b.Pop()
	}
	v, ok := b.table.PopBackFunc(func(item pqitem[T]) bool {
		return cond(item.value)
	})
	return v.value, ok
}

// Front returns next item in queue.
func (b *Prique[T]) Front() (T, bool) {
	v, ok := b.table.Back()
	return v.value, ok
}

// FrontMut returns next item in queue.
// MUTABLE OPERATION
func (b *Prique[T]) FrontMut() (T, bool) {
	v, ok := b.table.BackMut()
	return v.value, ok
}

// LookupAt returns the item at position, after first, in queue.
// Returns false if no item is found at position.
func (b *Prique[T]) LookupAt(pos int) (T, bool) {
	v, ok := b.table.LookupAt(b.table.Len() - pos - 1)
	return v.value, ok
}

// LookupAtMut returns the item at position, after first, in queue.
// Returns false if no item is found at position.
// MUTABLE OPERATION.
func (b *Prique[T]) LookupAtMut(pos int) (T, bool) {
	v, ok := b.table.LookupAtMut(b.table.Len() - pos - 1)
	return v.value, ok
}

// GetAt returns the item at position, after first, in queue.
func (b *Prique[T]) GetAt(pos int) T {
	v := b.table.GetAt(b.table.Len() - pos - 1)
	return v.value
}

// GetAtMut returns the item at position, after first, in queue.
// MUTABLE OPERATION.
func (b *Prique[T]) GetAtMut(pos int) T {
	v := b.table.GetAtMut(b.table.Len() - pos - 1)
	return v.value
}

// Len returns the number of items in queue
func (b *Prique[T]) Len() int {
	return b.table.Len()
}

// Clone the queue.
// This is a fast O(1) operation using a copy-on-write method.
func (b *Prique[T]) Clone() *Prique[T] {
	b2 := new(Prique[T])
	*b2 = *b
	b2.table = *b.table.Clone()
	return b2
}

// Clear the queue.
func (b *Prique[T]) Clear() {
	b.table.Clear()
	b.seqid = 0
}

// Release clears the queue and release any references.
// This method is functionally equivalent to Clear() but is an optimization for
// collections that are cloned.
func (b *Prique[T]) Release() {
	b.table.Release()
	b.seqid = 0
}

// Contains return true if item exists in queue
func (b *Prique[T]) Contains(key T) bool {
	keyA := pqitem[T]{key, 0}
	v, ok := b.table.Seek(keyA)
	if ok {
		keyB := pqitem[T]{v.value, 0}
		c := b.table.tree.dataCompare(omit{}, keyA, omit{}, keyB)
		if c == 0 {
			return true
		}
	}
	return false
}

// String returns a string representation of queue
func (b *Prique[T]) String() string {
	return fmt.Sprintf("prique%v", slices.Collect(b.All()))
}
