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
// Array
////////////////////////////////////////////////////////////////////////////////

// ArrayOptions are options for NewArrayOptions()
type ArrayOptions[T any] struct {
	Copy    func(T) T // Optional copy item function
	Release func(T)   // Optional release item function
}

// Array is btree backed array for storing arbritary items.
type Array[T any] struct {
	tree tree[omit, T]
}

// NewArrayOptions returns a new array with options
func NewArrayOptions[T any](opts ArrayOptions[T]) *Array[T] {
	b := new(Array[T])
	b.tree.dataCopy = opts.Copy
	b.tree.dataRelease = opts.Release
	return b
}

// NewArray returns a new array.
func NewArray[T any]() *Array[T] {
	return NewArrayOptions(ArrayOptions[T]{})
}

// Insert an item at index, moving existing items over by one.
// Returns true if item was inserted, or false if index is out of bounds.
// Use index=0 to insert front and index=arr.Len() to insert back.
func (b *Array[T]) Insert(index int, item T) bool {
	return b.tree.InsertAt(index, omit{}, item)
}

// Replace an item at index.
// Returns tru if item was replaced
func (b *Array[T]) Replace(index int, item T) (T, bool) {
	_, value, ok := b.tree.ReplaceAt(index, omit{}, item)
	return value, ok
}

// Delete item at index.
// Returns item and true if inserted or false if index is out of bounds
func (b *Array[T]) Delete(index int) (T, bool) {
	_, value, ok := b.tree.DeleteAt(index)
	return value, ok
}

// Lookup returns item at index or false if out of bounds.
func (b *Array[T]) Lookup(index int) (T, bool) {
	_, value, ok := b.tree.LookupAt(index)
	return value, ok
}

// Lookup returns item at index or false if out of bounds.
// MUTABLE OPERTAION
func (b *Array[T]) LookupMut(index int) (T, bool) {
	_, value, ok := b.tree.LookupAtMut(index)
	return value, ok
}

// Get returns item at index.
func (b *Array[T]) Get(index int) T {
	_, value := b.tree.GetAt(index)
	return value
}

// Get returns item at index.
// MUTABLE OPERTAION
func (b *Array[T]) GetMut(index int) T {
	_, value := b.tree.GetAtMut(index)
	return value
}

// DeleteRangeOptions deletes a range of items and returns deleted items as
// an iterable slice.
func (b *Array[T]) DeleteRangeOptions(index, count int,
	opts DeleteRangeOptions,
) Slice[T] {
	return sliceV(b.tree.DeleteRangeAt(index, count, opts))
}

// DeleteRange deletes a range of items and returns deleted items as an
// iterable slice.
func (b *Array[T]) DeleteRange(index, count int) Slice[T] {
	return b.DeleteRangeOptions(index, count, DeleteRangeOptions{})
}

// Len returns the number item in array.
func (b *Array[T]) Len() int {
	return b.tree.Len()
}

// Clear the array
func (b *Array[T]) Clear() {
	b.tree.Clear()
}

// Release clears the array and release any references.
// This method is functionally equivalent to Clear() but is an optimization for
// collections that are cloned using Clone().
func (b *Array[T]) Release() {
	b.tree.Release()
}

// Clone the array.
// This is a fast O(1) operation using a copy-on-write method.
func (b *Array[T]) Clone() *Array[T] {
	t2 := new(Array[T])
	*t2 = *b
	b.tree.CloneInto(&t2.tree)
	return t2
}

// All returns an iterator over the items.
func (b *Array[T]) All() iter.Seq[T] {
	return iterV(b.tree.All())
}

// AllMut returns an iterator over the items.
// MUTABLE OPERATION
func (b *Array[T]) AllMut() iter.Seq[T] {
	return iterV(b.tree.AllMut())
}

// Backward returns an iterator over the items in reverse order
func (b *Array[T]) Backward() iter.Seq[T] {
	return iterV(b.tree.Backward())
}

// Backward returns an iterator over the items in reverse order
// MUTABLE OPERATION
func (b *Array[T]) BackwardMut() iter.Seq[T] {
	return iterV(b.tree.BackwardMut())
}

// Ascend returns an iterator starting at index.
func (b *Array[T]) Ascend(index int) iter.Seq[T] {
	return iterV(b.tree.AscendAt(index))
}

// AscendMut returns an iterator starting at index.
// MUTABLE OPERATION
func (b *Array[T]) AscendMut(index int) iter.Seq[T] {
	return iterV(b.tree.AscendAtMut(index))
}

// Descend returns an iterator starting at index, in reverse order.
func (b *Array[T]) Descend(index int) iter.Seq[T] {
	return iterV(b.tree.DescendAt(index))
}

// DescendMut returns an iterator starting at index, in reverse order.
// MUTABLE OPERATION
func (b *Array[T]) DescendMut(index int) iter.Seq[T] {
	return iterV(b.tree.DescendAtMut(index))
}

// PushFront adds item to front of array.
func (b *Array[T]) PushFront(item T) {
	b.tree.PushFront(omit{}, item)
}

// PushBack adds item to back of array.
func (b *Array[T]) PushBack(item T) {
	b.tree.PushBack(omit{}, item)
}

// Front returns the first item in array.
func (b *Array[T]) Front() (T, bool) {
	_, value, ok := b.tree.Front()
	return value, ok
}

// FrontMut returns the first item in array.
// MUTABLE OPERATION
func (b *Array[T]) FrontMut() (T, bool) {
	_, value, ok := b.tree.FrontMut()
	return value, ok
}

// Back returns the last item in array.
func (b *Array[T]) Back() (T, bool) {
	_, value, ok := b.tree.Back()
	return value, ok
}

// BackMut returns the last item in array.
// MUTABLE OPERATION
func (b *Array[T]) BackMut() (T, bool) {
	_, value, ok := b.tree.BackMut()
	return value, ok
}

// PopFront removes the first item in array.
func (b *Array[T]) PopFront() (T, bool) {
	_, value, ok := b.tree.PopFront()
	return value, ok
}

// PopBack removes the last item in array.
func (b *Array[T]) PopBack() (T, bool) {
	_, value, ok := b.tree.PopBack()
	return value, ok
}

// PopFrontFunc removed first item if allowed by the provided function.
func (b *Array[T]) PopFrontFunc(cond func(item T) bool) (T, bool) {
	if cond == nil {
		return b.PopFront()
	}
	_, value, ok := b.tree.PopFrontFunc(func(_ omit, item T) bool {
		return cond(item)
	})
	return value, ok
}

// PopBackFunc removed last item if allowed by the provided function.
func (b *Array[T]) PopBackFunc(cond func(item T) bool) (T, bool) {
	if cond == nil {
		return b.PopBack()
	}
	_, value, ok := b.tree.PopBackFunc(func(_ omit, item T) bool {
		return cond(item)
	})
	return value, ok
}

// Append items to array
func (b *Array[T]) Append(items ...T) {
	for _, item := range items {
		b.PushBack(item)
	}
}

// Drain removes all items and returns them as an iterable sequence.
func (b *Array[T]) Drain() iter.Seq[T] {
	return iterV(b.tree.Drain())
}

// DrainBackward removes all items and returns them as an iterable sequence in
// reverse order.
func (b *Array[T]) DrainBackward() iter.Seq[T] {
	return iterV(b.tree.DrainBackward())
}

// Contains checks if array contains item.
func (b *Array[T]) Contains(item T) bool {
	cmp := CompareFor[T]()
	for vitem := range b.All() {
		if cmp(vitem, item) == 0 {
			return true
		}
	}
	return false
}

// String returns a string representation of array.
func (b *Array[T]) String() string {
	return "Array[]"
}
