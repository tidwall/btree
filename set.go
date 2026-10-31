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
// Set
////////////////////////////////////////////////////////////////////////////////

// Set is a b-tree set of keys
type Set[K cmp.Ordered] struct {
	base Map[K, omit]
}

// NewSet returns a new set
func NewSet[K cmp.Ordered]() *Set[K] {
	return new(Set[K])
}

// Insert a key
func (b *Set[K]) Insert(key K) bool {
	_, inserted := b.base.Insert(key, omit{})
	return inserted
}

// Delete a key
func (b *Set[K]) Delete(key K) bool {
	_, deleted := b.base.Delete(key)
	return deleted
}

// Contains returns true if key exists
func (b *Set[K]) Contains(key K) bool {
	return b.base.Contains(key)
}

// InsertAt inserts key at index.
// Returns false if out of order or out of bounds
func (b *Set[K]) InsertAt(index int, key K) bool {
	return b.base.InsertAt(index, key, omit{})
}

// DeleteAt deletes key at index
func (b *Set[K]) DeleteAt(index int) (K, bool) {
	key, _, ok := b.base.DeleteAt(index)
	return key, ok
}

// ReplaceAt replaces key at index.
func (b *Set[K]) ReplaceAt(index int, key K) (K, bool) {
	key, _, ok := b.base.ReplaceAt(index, key, omit{})
	return key, ok
}

// LookupAt returns key at index.
func (b *Set[K]) LookupAt(index int) (K, bool) {
	key, _, ok := b.base.LookupAt(index)
	return key, ok
}

// GetAt returns key at index
func (b *Set[K]) GetAt(index int) K {
	key, _ := b.base.GetAt(index)
	return key
}

// IndexOf returns the index of key.
func (b *Set[K]) IndexOf(key K) (int, bool) {
	return b.base.IndexOf(key)
}

// Ascend returns an iterator starting at index.
func (b *Set[K]) Ascend(key K) iter.Seq[K] {
	return iterK(b.base.Ascend(key))
}

// Descend returns an iterator starting at index in reverse order.
func (b *Set[K]) Descend(key K) iter.Seq[K] {
	return iterK(b.base.Descend(key))
}

// All returns an iterator of all keys.
func (b *Set[K]) All() iter.Seq[K] {
	return iterK(b.base.All())
}

// Backward returns an iterator of all keys in reverse order.
func (b *Set[K]) Backward() iter.Seq[K] {
	return iterK(b.base.Backward())
}

// AscendAt returns iterator starting at index.
func (b *Set[K]) AscendAt(index int) iter.Seq[K] {
	return iterK(b.base.AscendAt(index))
}

// AscendAt returns iterator starting index in reverse order
func (b *Set[K]) DescendAt(index int) iter.Seq[K] {
	return iterK(b.base.DescendAt(index))
}

// Seek returns the first key that is >= provided key.
func (b *Set[K]) Seek(key K) (K, bool) {
	key, _, ok := b.base.Seek(key)
	return key, ok
}

// SeekNext returns first key that is > provided key.
func (b *Set[K]) SeekNext(key K) (K, bool) {
	key, _, ok := b.base.SeekNext(key)
	return key, ok
}

// SeekPrev returns first key that is < provided key.
func (b *Set[K]) SeekPrev(key K) (K, bool) {
	key, _, ok := b.base.SeekPrev(key)
	return key, ok
}

// PushFront pushes key to front of set
func (b *Set[K]) PushFront(key K) bool {
	return b.base.PushFront(key, omit{})
}

// PushBack pushes key to back of set
func (b *Set[K]) PushBack(key K) bool {
	return b.base.PushBack(key, omit{})
}

// PopFront removes first key
func (b *Set[K]) PopFront() (K, bool) {
	key, _, ok := b.base.PopFront()
	return key, ok
}

// PopBack removes last key
func (b *Set[K]) PopBack() (K, bool) {
	key, _, ok := b.base.PopBack()
	return key, ok
}

// Front returns first key
func (b *Set[K]) Front() (K, bool) {
	key, _, ok := b.base.Front()
	return key, ok
}

// Back returns last key
func (b *Set[K]) Back() (K, bool) {
	key, _, ok := b.base.Back()
	return key, ok
}

// Len returns the number of keys in set
func (b *Set[K]) Len() int {
	return b.base.Len()
}

// Clear the set
func (b *Set[K]) Clear() {
	b.base.Clear()
}

// Clone the set
// This is a fast O(1) operation using a copy-on-write method.
func (b *Set[K]) Clone() *Set[K] {
	b2 := new(Set[K])
	*b2 = *b
	b.base.cloneInto(&b2.base)
	return b2
}

// Release clears the set and release any references.
// This method is functionally equivalent to Clear() but is an optimization for
// collections that are cloned.
func (b *Set[K]) Release() {
	b.base.Release()
}

// DrainAtOptions will delete all items within the provided index and
// count sub-range.
// Returns the deleted items as an ordered extract that can be iterated over.
func (b *Set[K]) DrainAtOptions(index, count int, opts DrainOptions,
) Extract[K] {
	return sliceK(b.base.DrainAtOptions(index, count, opts))
}

// DrainOptions will delete all items within the provided min (inclusive)
// and max (exclusive) sub-range.
// Returns the deleted items as an ordered extract that can be iterated over.
func (b *Set[K]) DrainOptions(min, max K, opts DrainOptions,
) Extract[K] {
	return sliceK(b.base.DrainOptions(min, max, opts))
}

// DeleteRangeOptions will delete all items within the provided min (inclusive)
// and max (exclusive) sub-range.
// Returns the deleted items as an ordered extract that can be iterated over.
func (b *Set[K]) DrainAt(index, count int) Extract[K] {
	return b.DrainAtOptions(index, count, DrainOptions{})
}

// Drain will delete all items within the provided min (inclusive) and
// max (exclusive) sub-range.
// Returns the deleted items as an ordered extract that can be iterated over.
func (b *Set[K]) Drain(min, max K) Extract[K] {
	return b.DrainOptions(min, max, DrainOptions{})
}

// String representation
func (b *Set[K]) String() string {
	return fmt.Sprintf("set%v", slices.Collect(b.All()))
}

// Equal returs true if boths sets contains the same items
func (b *Set[K]) Equal(other *Set[K]) bool {
	if b == other {
		return true
	}
	const group = 128 // gather no more than this many items at a time
	var g1 []K
	var g2 []K
	var start int
	for {
		g1 = g1[:0]
		for k := range b.AscendAt(start) {
			if len(g1) == group {
				break
			}
			g1 = append(g1, k)
		}
		g2 = g2[:0]
		for k := range other.AscendAt(start) {
			if len(g2) == group {
				break
			}
			g2 = append(g2, k)
		}
		if !slices.Equal(g1, g2) {
			return false
		}
		if len(g1) < group {
			return true
		}
		start += group
	}
}

// DeleteFunc delete item if its allowed by del function.
func (b *Set[K]) DeleteFunc(del func(K) bool) bool {
	return b.base.DeleteFunc(func(k K, v omit) bool { return del(k) })
}

// UnionWith inserts all keys of other set.
func (b *Set[K]) UnionWith(other *Set[K]) {
	if b != other {
		for k := range other.All() {
			b.Insert(k)
		}
	}
}

// Union returns a new set containing all keys of both sets.
func (b *Set[K]) Union(other *Set[K]) *Set[K] {
	b2 := b.Clone()
	if b != other {
		b2.UnionWith(other)
	}
	return b2
}

// Difference returns a new set containing keys that are in target set but not
// in other.
func (b *Set[K]) Difference(other *Set[K]) *Set[K] {
	b2 := NewSet[K]()
	if b != other {
		for k := range b.All() {
			if !other.Contains(k) {
				b2.Insert(k)
			}
		}
	}
	return b2
}

// DifferenceWith removes any items from set that are in other.
func (b *Set[K]) DifferenceWith(other *Set[K]) {
	if b == other {
		b.Clear()
	} else {
		for k := range other.All() {
			b.Delete(k)
		}
	}
}

// Intersection returns a new set containing item that are in both sets.
func (b *Set[K]) Intersection(other *Set[K]) *Set[K] {
	if b == other {
		return b.Clone()
	}
	b2 := NewSet[K]()
	for k := range b.All() {
		if other.Contains(k) {
			b2.Insert(k)
		}
	}
	return b2
}

// IntersectionWith removes any items from set that are not in other.
func (b *Set[K]) IntersectionWith(other *Set[K]) {
	if b == other {
		return
	}
	for k := range b.Clone().All() {
		if !other.Contains(k) {
			b.Delete(k)
		}
	}
}

// Intersects returns true if other and s share any items.
func (b *Set[K]) Intersects(other *Set[K]) bool {
	if b == other {
		return other.Len() > 0
	}
	if b.Len() >= other.Len() {
		for k := range other.All() {
			if b.Contains(k) {
				return true
			}
		}
		return false
	}
	return other.Intersects(b)
}

// SymmetricDifference returns a new set containing items that are in either,
// but not both.
func (b *Set[K]) SymmetricDifference(other *Set[K]) *Set[K] {
	b2 := NewSet[K]()
	if b == other {
		return b2
	}
	for k := range b.All() {
		if !other.Contains(k) {
			b2.Insert(k)
		}
	}
	for k := range other.All() {
		if !b.Contains(k) {
			b2.Insert(k)
		}
	}
	return b2
}

// SymmetricDifferenceWith update set to the symmetric difference of both sets
func (b *Set[K]) SymmetricDifferenceWith(other *Set[K]) {
	if b == other {
		b.Clear()
	} else {
		for k := range other.All() {
			if b.Contains(k) {
				b.Delete(k)
			} else {
				b.Insert(k)
			}
		}
	}
}
