// https://github.com/tidwall/btree
//
// Copyright 2026 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
//
// B-tree based collections for go
package btree // import "github.com/tidwall/btree/v2"

import "iter"

////////////////////////////////////////////////////////////////////////////////
// Stack
////////////////////////////////////////////////////////////////////////////////

// Stack provides the functionality of a stack - specifically, a
// LIFO (last-in, first-out) data structure.
type Stack[T any] struct {
	tree tree[omit, T]
}

// StackOptions for NewStackOptions()
type StackOptions[T any] struct {
	Copy    func(T) T
	Release func(T)
}

// NewStackOptions returns a new stack
func NewStackOptions[T any](opts StackOptions[T]) *Stack[T] {
	b := new(Stack[T])
	b.tree.dataCopy = opts.Copy
	b.tree.dataRelease = opts.Release
	return b
}

// NewStack returns a new stack
func NewStack[T any]() *Stack[T] {
	return NewStackOptions(StackOptions[T]{})
}

// Push an item to top of stack.
func (b *Stack[T]) Push(item T) {
	b.tree.PushBack(omit{}, item)
}

// Pop the top item from top stack.
func (b *Stack[T]) Pop() (T, bool) {
	_, item, ok := b.tree.PopBack()
	return item, ok
}

// Top returns the top item in stack, or false if stack is empty.
func (b *Stack[T]) Top() (T, bool) {
	_, item, ok := b.tree.Back()
	return item, ok
}

// TopMut returns the top item in stack, or false if stack is empty.
// MUTABLE OPERATION.
func (b *Stack[T]) TopMut() (T, bool) {
	_, item, ok := b.tree.BackMut()
	return item, ok
}

// At returns the item At position, after top, in Stack.
// Returns false if no item is found At position.
func (b *Stack[T]) At(pos int) (T, bool) {
	_, item, ok := b.tree.LookupAt(b.tree.Len() - pos - 1)
	return item, ok
}

// AtMut returns the item at position, after top, in Stack.
// Returns false if no item is found at position.
// MUTABLE OPERATION.
func (b *Stack[T]) AtMut(pos int) (T, bool) {
	_, item, ok := b.tree.LookupAtMut(b.tree.Len() - pos - 1)
	return item, ok
}

// Len returns the number of items in stack.
func (b *Stack[T]) Len() int {
	return b.tree.Len()
}

// Clone the stack.
// This is a fast O(1) operation using a copy-on-write method.
func (b *Stack[T]) Clone() *Stack[T] {
	b2 := new(Stack[T])
	*b2 = *b
	b.tree.CloneInto(&b2.tree)
	return b2
}

// Clear the stack.
func (b *Stack[T]) Clear() {
	b.tree.Clear()
}

// Release clears the stack and releases any copied reference.
// This method is functionally equivalent to Clear() but is an optimization for
// collections that are copied using Clone().
func (b *Stack[T]) Release() {
	b.tree.Release()
}

// All returns an iterator of all items starting from the top of the stack.
func (b *Stack[T]) All() iter.Seq[T] {
	return iterV(b.tree.Backward())
}

// AllMut returns an iterator of all items starting from the top of the stack.
// MUTABLE OPERATION.
func (b *Stack[T]) AllMut() iter.Seq[T] {
	return iterV(b.tree.BackwardMut())
}

func (b *Stack[K]) Drain() iter.Seq[K] {
	return iterV(b.tree.DrainBackward())
}

func (b *Stack[K]) Contains(item K) bool {
	cmp := CompareFor[K]()
	for vitem := range b.All() {
		if cmp(vitem, item) == 0 {
			return true
		}
	}
	return false
}

func (b *Stack[K]) String() string {
	return "Stack[]"
}
