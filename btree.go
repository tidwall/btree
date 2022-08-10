// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
package btree

type (
	Integer any

	Item[K Integer] interface {
		Weight() K
	}

	BTree struct {
		base *Generic[Item[Integer], Integer]
	}
)

// New returns a new BTree
func New(less func(a, b Item[Integer]) bool, accumulate func(a, b Integer) Integer) *BTree {
	if less == nil {
		panic("nil less")
	}
	return &BTree{
		base: NewGeneric(less, accumulate),
	}
}

// NewNonConcurrent returns a new BTree which is not safe for concurrent
// write operations by multiple goroutines.
//
// This is useful for when you do not need the BTree to manage the locking,
// but would rather do it yourself.
func NewNonConcurrent(less func(a, b Item[Integer]) bool, accumulate func(a, b Integer) Integer) *BTree {
	if less == nil {
		panic("nil less")
	}
	return &BTree{
		base: NewGenericOptions(less, accumulate, Options{NoLocks: true}),
	}
}

// Less is a convenience function that performs a comparison of two items
// using the same "less" function provided to New.
func (tr *BTree) Less(a, b Item[Integer]) bool {
	return tr.base.Less(a, b)
}

// Set or replace a value for a key
// Returns the value for the replaced item or nil if the key was not found.
func (tr *BTree) Set(item Item[Integer]) (prev Item[Integer]) {
	return tr.SetHint(item, nil)
}

// SetHint sets or replace a value for a key using a path hint
// Returns the value for the replaced item or nil if the key was not found.
func (tr *BTree) SetHint(item Item[Integer], hint *PathHint) (prev Item[Integer]) {
	if item == nil {
		panic("nil item")
	}
	v, ok := tr.base.SetHint(item, hint)
	if !ok {
		return nil
	}
	return v
}

// Get a value for key.
// Returns nil if the key was not found.
func (tr *BTree) Get(key Item[Integer]) Item[Integer] {
	return tr.GetHint(key, nil)
}

// GetHint gets a value for key using a path hint.
// Returns nil if the item was not found.
func (tr *BTree) GetHint(key Item[Integer], hint *PathHint) (value Item[Integer]) {
	if key == nil {
		return nil
	}
	v, ok := tr.base.GetHint(key, hint)
	if !ok {
		return nil
	}
	return v
}

// Len returns the number of items in the tree
func (tr *BTree) Len() int {
	return tr.base.Len()
}

// Delete an item for a key.
// Returns the deleted value or nil if the key was not found.
func (tr *BTree) Delete(key Item[Integer]) (prev Item[Integer]) {
	return tr.DeleteHint(key, nil)
}

// DeleteHint deletes a value for a key using a path hint
// Returns the deleted value or nil if the key was not found.
func (tr *BTree) DeleteHint(key Item[Integer], hint *PathHint) (prev Item[Integer]) {
	if key == nil {
		return nil
	}
	v, ok := tr.base.DeleteHint(key, nil)
	if !ok {
		return nil
	}
	return v
}

// Ascend the tree within the range [pivot, last]
// Pass nil for pivot to scan all item in ascending order
// Return false to stop iterating
func (tr *BTree) Ascend(pivot Item[Integer], iter func(item Item[Integer]) bool) {
	if pivot == nil {
		tr.base.Scan(iter)
	} else {
		tr.base.Ascend(pivot, iter)
	}
}

// Descend the tree within the range [pivot, first]
// Pass nil for pivot to scan all item in descending order
// Return false to stop iterating
func (tr *BTree) Descend(pivot Item[Integer], iter func(item Item[Integer]) bool) {
	if pivot == nil {
		tr.base.Reverse(iter)
	} else {
		tr.base.Descend(pivot, iter)
	}
}

// Load is for bulk loading pre-sorted items
// If the load replaces and existing item then the value for the replaced item
// is returned.
func (tr *BTree) Load(item Item[Integer]) (prev Item[Integer]) {
	if item == nil {
		panic("nil item")
	}
	v, ok := tr.base.Load(item)
	if !ok {
		return nil
	}
	return v
}

// Min returns the minimum item in tree.
// Returns nil if the tree has no items.
func (tr *BTree) Min() Item[Integer] {
	v, ok := tr.base.Min()
	if !ok {
		return nil
	}
	return v
}

// Max returns the maximum item in tree.
// Returns nil if the tree has no items.
func (tr *BTree) Max() Item[Integer] {
	v, ok := tr.base.Max()
	if !ok {
		return nil
	}
	return v
}

// PopMin removes the minimum item in tree and returns it.
// Returns nil if the tree has no items.
func (tr *BTree) PopMin() Item[Integer] {
	v, ok := tr.base.PopMin()
	if !ok {
		return nil
	}
	return v
}

// PopMax removes the maximum item in tree and returns it.
// Returns nil if the tree has no items.
func (tr *BTree) PopMax() Item[Integer] {
	v, ok := tr.base.PopMax()
	if !ok {
		return nil
	}
	return v
}

// GetAt returns the value at index.
// Return nil if the tree is empty or the index is out of bounds.
func (tr *BTree) GetAt(index int) Item[Integer] {
	v, ok := tr.base.GetAt(index)
	if !ok {
		return nil
	}
	return v
}

// DeleteAt deletes the item at index.
// Return nil if the tree is empty or the index is out of bounds.
func (tr *BTree) DeleteAt(index int) Item[Integer] {
	v, ok := tr.base.DeleteAt(index)
	if !ok {
		return nil
	}
	return v
}

// Height returns the height of the tree.
// Returns zero if tree has no items.
func (tr *BTree) Height() int {
	return tr.base.Height()
}

// Walk iterates over all items in tree, in order.
// The items param will contain one or more items.
func (tr *BTree) Walk(iter func(items []Item[Integer])) {
	tr.base.Walk(func(items []Item[Integer]) bool {
		iter(items)
		return true
	})
}

// Copy the tree. This is a copy-on-write operation and is very fast because
// it only performs a shadowed copy.
func (tr *BTree) Copy() *BTree {
	return &BTree{base: tr.base.Copy()}
}

type Iter struct {
	base GenericIter[Item[Integer], Integer]
}

// Iter returns a read-only iterator.
// The Release method must be called finished with iterator.
func (tr *BTree) Iter() Iter {
	return Iter{tr.base.Iter()}
}

// Seek to item greater-or-equal-to key.
// Returns false if there was no item found.
func (iter *Iter) Seek(key Item[Integer]) bool {
	return iter.base.Seek(key)
}

// First moves iterator to first item in tree.
// Returns false if the tree is empty.
func (iter *Iter) First() bool {
	return iter.base.First()
}

// Last moves iterator to last item in tree.
// Returns false if the tree is empty.
func (iter *Iter) Last() bool {
	return iter.base.Last()
}

// First moves iterator to first item in tree.
// Returns false if the tree is empty.
func (iter *Iter) Release() {
	iter.base.Release()
}

// Next moves iterator to the next item in iterator.
// Returns false if the tree is empty or the iterator is at the end of
// the tree.
func (iter *Iter) Next() bool {
	return iter.base.Next()
}

// Prev moves iterator to the previous item in iterator.
// Returns false if the tree is empty or the iterator is at the beginning of
// the tree.
func (iter *Iter) Prev() bool {
	return iter.base.Prev()
}

// Item returns the current iterator item.
func (iter *Iter) Item() Item[Integer] {
	return iter.base.Item()
}
