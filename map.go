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
	"unsafe"
)

////////////////////////////////////////////////////////////////////////////////
// Map
////////////////////////////////////////////////////////////////////////////////

// Map is sorted collection of key value pairs
type Map[K cmp.Ordered, V any] struct {
	tree tree[K, V]
}

// MapOptions for NewMapOptions
type MapOptions[K cmp.Ordered, V any] struct {
	Copy    func(V) V // Optional copy function
	Release func(V)   // Optional release function
}

// NewMapOptions returns a new map
func NewMapOptions[K cmp.Ordered, V any](opts MapOptions[K, V]) *Map[K, V] {
	b := new(Map[K, V])
	b.tree.dataCopy = opts.Copy
	b.tree.dataRelease = opts.Release
	b.init()
	return b
}

// NewMap returns a new map
func NewMap[K cmp.Ordered, V any]() *Map[K, V] {
	return NewMapOptions(MapOptions[K, V]{})
}

func mapCompare[K cmp.Ordered, V any](akey K, aval V, bkey K, bval V) int {
	return cmp.Compare(akey, bkey)
}

func mapKeyAt[K any](p *K, i int) (k K) {
	return *(*K)(unsafe.Add(unsafe.Pointer(p), i*int(unsafe.Sizeof(k))))
}

func (n *node[K, V]) keyAt(i int) (k K) {
	return mapKeyAt(&n.keys[0], i)
}

func mapSearch[K cmp.Ordered, V any](len int, p *K, _ *V, key K, _ V,
) (i int, found bool) {
	if key > mapKeyAt(p, len-1) {
		return len, false
	}
	for key > mapKeyAt(p, i) {
		i++
	}
	return i, key == mapKeyAt(p, i)
}

func mapBinarySearch[K cmp.Ordered, V any](len int, p *K, _ *V, key K, _ V,
) (i int, found bool) {
	if key > mapKeyAt(p, len-1) {
		return len, false
	}
	low, high := 0, len
	for low < high {
		h := (low + high) >> 1
		c := cmp.Compare(key, mapKeyAt(p, h))
		if c < 0 {
			high = h
		} else if c > 0 {
			low = h + 1
		} else {
			return h, true
		}
	}
	return low, false
}

//go:noinline
func (b *Map[K, V]) init0() {
	if b.tree.dataCompare == nil {
		b.tree.dataCompare = mapCompare
		var key K
		if _, ok := any(key).(string); ok {
			// Use standard operations with binary search.
			b.tree.dataSearch = mapBinarySearch
			b.tree.bops = true
		}
	}
	if b.tree.dataSearch == nil {
		b.tree.dataSearch = mapSearch
	}
	b.tree.initd = true
}

func (b *Map[K, V]) init() {
	// Called from Insert, InsertAt, Set, PushFront, PushBack
	if !b.tree.initd {
		b.init0()
	}
}

func (b *Map[K, V]) nodeInsert(n *node[K, V], key K, value V) (V, int) {
	var emptyValue V
	// only branch nodes are reachable here
	var child *node[K, V]
	var i int
	var j int
	if key > n.keyAt(n.len-1) {
		i = n.len
	} else {
		for key > n.keyAt(i) {
			i++
		}
		if key == n.keyAt(i) {
			goto foundInBranch
		}
	}
	if b.tree.cloned {
		b.tree.cowChild(n, i, true)
	}
	if b.tree.ensureBranch(n, i) {
		return b.nodeInsert(n, key, value)
	}
	child = n.branch.children[i]
	if !child.leaf() {
		prev, inserted := b.nodeInsert(child, key, value)
		n.branch.counts[i] += inserted
		return prev, inserted
	}
	// skip the last recursive call
	if key > child.keyAt(child.len-1) {
		j = child.len
	} else {
		for key > child.keyAt(j) {
			j++
		}
		if key == child.keyAt(j) {
			goto foundInLeaf
		}
	}
	child.insertItemAt(key, value, j)
	n.branch.counts[i]++
	return emptyValue, 1
foundInBranch:
	return n.values[i], 0
foundInLeaf:
	return child.values[j], 0
}

func mapInsert[K cmp.Ordered, V any](b *Map[K, V], key K, value V) (V, bool) {
	b.init()
	var emptyValue V
	if b.tree.count == 0 {
		b.tree.insertFirstItem(key, value)
		return emptyValue, true
	}
	if b.tree.cloned {
		b.tree.cowRoot(true)
	}
	b.tree.ensureRoot()
	if !b.tree.root.leaf() {
		current, inserted := b.nodeInsert(b.tree.root, key, value)
		b.tree.count += inserted
		return current, inserted == 1
	}
	i := 0
	if key > b.tree.root.keyAt(b.tree.root.len-1) {
		i = b.tree.root.len
	} else {
		for key > b.tree.root.keyAt(i) {
			i++
		}
		if key == b.tree.root.keyAt(i) {
			return b.tree.root.values[i], false
		}
	}
	b.tree.root.insertItemAt(key, value, i)
	b.tree.count++
	return emptyValue, true
}

func mapReplace[K cmp.Ordered, V any](b *Map[K, V], key K, value V) (V, bool) {
	var emptyValue V
	if b.tree.count == 0 {
		return emptyValue, false
	}
	b.tree.cowRoot(true)
	n := b.tree.root
	for {
		i := 0
		if key > n.keyAt(n.len-1) {
			i = n.len
		} else {
			for key > n.keyAt(i) {
				i++
			}
			if key == n.keyAt(i) {
				old := n.values[i]
				n.values[i] = value
				return old, true
			}
		}
		if n.leaf() {
			return emptyValue, false
		}
		b.tree.cowChild(n, i, true)
		n = n.branch.children[i]
	}
}

func (b *Map[K, V]) nodeSet(n *node[K, V], key K, value V) (V, int) {
	var old V
	// only branch nodes are reachable here
	var child *node[K, V]
	var i int
	var j int
	if key > n.keyAt(n.len-1) {
		i = n.len
	} else {
		for key > n.keyAt(i) {
			i++
		}
		if key == n.keyAt(i) {
			goto foundInBranch
		}
	}
	if b.tree.cloned {
		b.tree.cowChild(n, i, true)
	}
	if b.tree.ensureBranch(n, i) {
		return b.nodeSet(n, key, value)
	}
	child = n.branch.children[i]
	if !child.leaf() {
		prev, inserted := b.nodeSet(child, key, value)
		n.branch.counts[i] += inserted
		return prev, inserted
	}
	// skip the last recursive call
	if key > child.keyAt(child.len-1) {
		j = child.len
	} else {
		for key > child.keyAt(j) {
			j++
		}
		if key == child.keyAt(j) {
			goto foundInLeaf
		}
	}
	child.insertItemAt(key, value, j)
	n.branch.counts[i]++
	return old, 1
foundInLeaf:
	old = child.values[j]
	child.values[j] = value
	return old, 0
foundInBranch:
	old = n.values[i]
	n.values[i] = value
	return old, 0
}

func mapSet[K cmp.Ordered, V any](b *Map[K, V], key K, value V) (V, bool) {
	b.init()
	var emptyValue V
	if b.tree.count == 0 {
		b.tree.insertFirstItem(key, value)
		return emptyValue, false
	}
	if b.tree.cloned {
		b.tree.cowRoot(true)
	}
	b.tree.ensureRoot()
	if !b.tree.root.leaf() {
		current, inserted := b.nodeSet(b.tree.root, key, value)
		b.tree.count += inserted
		return current, inserted == 0
	}
	i := 0
	if key > b.tree.root.keyAt(b.tree.root.len-1) {
		i = b.tree.root.len
	} else {
		for key > b.tree.root.keyAt(i) {
			i++
		}
		if key == b.tree.root.keyAt(i) {
			prev := b.tree.root.values[i]
			b.tree.root.values[i] = value
			return prev, true
		}
	}
	b.tree.root.insertItemAt(key, value, i)
	b.tree.count++
	return emptyValue, false
}

func mapLookup[K cmp.Ordered, V any](b *Map[K, V], key K) (V, bool) {
	var emptyValue V
	if b.tree.count == 0 {
		return emptyValue, false
	}
	n := b.tree.root
	for {
		i := 0
		if key > n.keyAt(n.len-1) {
			i = n.len
		} else {
			for key > n.keyAt(i) {
				i++
			}
			if key == n.keyAt(i) {
				return n.values[i], true
			}
		}
		if n.leaf() {
			return emptyValue, false
		}
		n = n.branch.children[i]
	}
}

func mapLookupMut[K cmp.Ordered, V any](b *Map[K, V], key K) (V, bool) {
	var emptyValue V
	if b.tree.count == 0 {
		return emptyValue, false
	}
	b.tree.cowRoot(true)
	n := b.tree.root
	for {
		i := 0
		if key > n.keyAt(n.len-1) {
			i = n.len
		} else {
			for key > n.keyAt(i) {
				i++
			}
			if key == n.keyAt(i) {
				return n.values[i], true
			}
		}
		if n.leaf() {
			return emptyValue, false
		}
		b.tree.cowChild(n, i, true)
		n = n.branch.children[i]
	}
}

func (b *Map[K, V]) nodeDelete(n *node[K, V], key K) (V, bool) {
	var emptyKey K
	var emptyValue V
	i, found := 0, false
	if key > n.keyAt(n.len-1) {
		i = n.len
	} else {
		for key > n.keyAt(i) {
			i++
		}
		found = key == n.keyAt(i)
	}
	if n.leaf() {
		if found {
			old := n.values[i]
			copy(n.keys[i:n.len-1], n.keys[i+1:n.len])
			n.keys[n.len-1] = emptyKey
			copy(n.values[i:n.len-1], n.values[i+1:n.len])
			n.values[n.len-1] = emptyValue
			n.len--
			return old, true
		}
		return emptyValue, false
	}
	var old V
	var deleted bool
	b.tree.cowChild(n, i, true)
	if found {
		old = n.values[i]
		maxKey, maxValue := b.tree.nodeDeleteMax(n.branch.children[i])
		deleted = true
		n.keys[i] = maxKey
		n.values[i] = maxValue
	} else {
		old, deleted = b.nodeDelete(n.branch.children[i], key)
	}
	if !deleted {
		return old, false
	}
	n.branch.counts[i]--
	if n.branch.children[i].len < minItems {
		b.tree.rebalance(n, i)
	}
	return old, true
}

func mapDelete[K cmp.Ordered, V any](b *Map[K, V], key K) (V, bool) {
	var emptyValue V
	if b.tree.count == 0 {
		return emptyValue, false
	}
	b.tree.cowRoot(true)
	old, deleted := b.nodeDelete(b.tree.root, key)
	if deleted {
		b.tree.count--
		if b.tree.count == 0 {
			b.tree.root = nil
		} else if b.tree.root.len == 0 && !b.tree.root.leaf() {
			b.tree.root = b.tree.root.branch.children[0]
		}
	}
	return old, deleted
}

// Insert new item.
// Returns true if inserted or false if already exists.
func (b *Map[K, V]) Insert(key K, value V) (V, bool) {
	if b.tree.bops {
		return b.tree.Insert(key, value)
	}
	return mapInsert(b, key, value)
}

// Replace existing item
// Returns true if replaced, of false if not exists.
func (b *Map[K, V]) Replace(key K, value V) (V, bool) {
	if b.tree.bops {
		return b.tree.Replace(key, value)
	}
	return mapReplace(b, key, value)
}

// Set inserts or replaces item.
// Returns true if replaced or false if inserted
func (b *Map[K, V]) Set(key K, value V) (V, bool) {
	if b.tree.bops {
		return b.tree.Set(key, value)
	}
	return mapSet(b, key, value)
}

// Delete item
func (b *Map[K, V]) Delete(key K) (V, bool) {
	if b.tree.bops {
		var emptyValue V
		return b.tree.Delete(key, emptyValue)
	}
	return mapDelete(b, key)
}

// Contains returns true if item exists
func (b *Map[K, V]) Contains(key K) bool {
	if b.tree.count == 0 {
		return false
	}
	n := b.tree.root
	for {
		i := 0
		if key > n.keyAt(n.len-1) {
			i = n.len
		} else {
			for key > n.keyAt(i) {
				i++
			}
			if key == n.keyAt(i) {
				return true
			}
		}
		if n.leaf() {
			return false
		}
		n = n.branch.children[i]
	}
}

// Lookup an item
// Returns true if found or false if not found
func (b *Map[K, V]) Lookup(key K) (V, bool) {
	if b.tree.bops {
		var emptyValue V
		return b.tree.Lookup(key, emptyValue)
	}
	return mapLookup(b, key)
}

// Lookup an item
// Returns true if found or false if not found
// MUTABLE OPERATION
func (b *Map[K, V]) LookupMut(key K) (V, bool) {
	if b.tree.bops {
		var emptyValue V
		return b.tree.LookupMut(key, emptyValue)
	}
	return mapLookupMut(b, key)
}

// Get an item
func (b *Map[K, V]) Get(key K) V {
	v, _ := b.Lookup(key)
	return v
}

// Action type used by the Action() function
type Action int

const (
	// ActionGet takes no action, just a simple Get.
	ActionGet Action = iota
	// ActionDelete deletes item
	ActionDelete
	// ActionReplace replaces item
	ActionReplace
)

// Action performs an action on item with key.
// The "action" function is called when the item is found.
// Returning an Action type from action function to:
// - ActionGet: do nothing
// - ActionDelete: delete item
// - ActionReplace: replace with returned value
func (b *Map[K, V]) Action(key K, action func(V) (V, Action)) {
	var emptyKey K
	var emptyValue V
	if b.tree.count == 0 || action == nil {
		return
	}
	b.tree.cowRoot(true)
	var path [24]byte
	var depth int
	n := b.tree.root
	for {
		i := 0
		if key > n.keyAt(n.len-1) {
			i = n.len
		} else {
			for key > n.keyAt(i) {
				i++
			}
			if key == n.keyAt(i) {
				val, act := action(n.values[i])
				switch act {
				case ActionGet:
					// do nothing
				case ActionReplace:
					// replace in-place
					n.values[i] = val
				case ActionDelete:
					if fanout < 256 && n.leaf() && n.len > minItems {
						// delete in-place and fixup counts
						copy(n.keys[i:n.len-1], n.keys[i+1:n.len])
						n.keys[n.len-1] = emptyKey
						copy(n.values[i:n.len-1], n.values[i+1:n.len])
						n.values[n.len-1] = emptyValue
						n.len--
						b.tree.count--
						n := b.tree.root
						for i := 0; i < depth; i++ {
							n.branch.counts[path[i]]--
							n = n.branch.children[path[i]]
						}
					} else {
						// fallback to standard deletion
						b.Delete(key)
					}
				}
				return
			}
		}
		if n.leaf() {
			return
		}
		b.tree.cowChild(n, i, true)
		n = n.branch.children[i]
		path[depth] = byte(i)
		depth++
	}
}

// Ascend returns an iterator starting at key
func (b *Map[K, V]) Ascend(key K) iter.Seq2[K, V] {
	var emptyValue V
	return b.tree.Ascend(key, emptyValue)
}

// AscendMut returns an iterator starting at key
// MUTABLE OPERATION
func (b *Map[K, V]) AscendMut(key K) iter.Seq2[K, V] {
	var emptyValue V
	return b.tree.AscendMut(key, emptyValue)
}

// Len returns the number of items in map
func (b *Map[K, V]) Len() int {
	return b.tree.Len()
}

// Clear the map
func (b *Map[K, V]) Clear() {
	b.tree.Clear()
}

// Release clears the map and release any references.
// This method is functionally equivalent to Clear() but is an optimization for
// collections that are cloned.
func (b *Map[K, V]) Release() {
	b.tree.Release()
}

func (b *Map[K, V]) cloneInto(b2 *Map[K, V]) {
	*b2 = *b
	b.tree.CloneInto(&b2.tree)
}

// Clone the map.
// This is a fast O(1) operation using a copy-on-write method.
func (b *Map[K, V]) Clone() *Map[K, V] {
	b2 := new(Map[K, V])
	b.cloneInto(b2)
	return b2
}

// DrainAtOptions will delete all items within the provided index and
// count sub-range.
// Returns the deleted items as an ordered extract that can be iterated over.
func (b *Map[K, V]) DrainAtOptions(index, count int, opts DrainOptions,
) Extract2[K, V] {
	return b.tree.DrainAt(index, count, opts)
}

// DrainOptions will delete all items within the provided min (inclusive)
// and max (exclusive) sub-range.
// Returns the deleted items as an ordered extract that can be iterated over.
func (b *Map[K, V]) DrainOptions(min, max K, opts DrainOptions) Extract2[K, V] {
	var emptyValue V
	return b.tree.Drain(min, emptyValue, max, emptyValue, opts)
}

// DrainAt will delete all items within the provided index and count
// sub-range.
// Returns the deleted items as an ordered extract that can be iterated over.
func (b *Map[K, V]) DrainAt(index, count int) Extract2[K, V] {
	return b.DrainAtOptions(index, count, DrainOptions{})
}

// Drain will delete all items within the provided min (inclusive) and
// max (exclusive) sub-range.
// Returns the deleted items as an ordered extract that can be iterated over.
func (b *Map[K, V]) Drain(min, max K) Extract2[K, V] {
	return b.DrainOptions(min, max, DrainOptions{})
}

// Decend returns an iterator with items less than key, in reverse order.
func (b *Map[K, V]) Descend(key K) iter.Seq2[K, V] {
	var emptyValue V
	return b.tree.Descend(key, emptyValue)
}

// DescendMut returns an iterator with items less than key, in reverse order.
// MUTABLE OPERATION
func (b *Map[K, V]) DescendMut(key K) iter.Seq2[K, V] {
	var emptyValue V
	return b.tree.DescendMut(key, emptyValue)
}

// IndexOf returns the index of key
func (b *Map[K, V]) IndexOf(key K) (int, bool) {
	var emptyValue V
	return b.tree.IndexOf(key, emptyValue)
}

// All returns an iterator of all items in map.
func (b *Map[K, V]) All() iter.Seq2[K, V] {
	return b.tree.All()
}

// AllMut returns an iterator of all items in map.
// MUTABLE OPERATION
func (b *Map[K, V]) AllMut() iter.Seq2[K, V] {
	return b.tree.AllMut()
}

// Backward returns an iterator of all items in map, in reverse order.
func (b *Map[K, V]) Backward() iter.Seq2[K, V] {
	return b.tree.Backward()
}

// BackwardMut returns an iterator of all items in map, in reverse order.
// MUTABLE OPERATION
func (b *Map[K, V]) BackwardMut() iter.Seq2[K, V] {
	return b.tree.BackwardMut()
}

// AscendAt returns an iterator starting at index
func (b *Map[K, V]) AscendAt(index int) iter.Seq2[K, V] {
	return b.tree.AscendAt(index)
}

// AscendAtMut returns an iterator starting at index
// MUTABLE OPERATION
func (b *Map[K, V]) AscendAtMut(index int) iter.Seq2[K, V] {
	return b.tree.AscendAtMut(index)
}

// DescendAt returns an iterator starting at index, in reverse order
func (b *Map[K, V]) DescendAt(index int) iter.Seq2[K, V] {
	return b.tree.DescendAt(index)
}

// DescendAtMut returns an iterator starting at index, in reverse order
// MUTABLE OPERATION
func (b *Map[K, V]) DescendAtMut(index int) iter.Seq2[K, V] {
	return b.tree.DescendAtMut(index)
}

// PushFront insertes item to front of map.
func (b *Map[K, V]) PushFront(key K, value V) bool {
	b.init()
	return b.tree.PushFront(key, value)
}

// PushBack insertes item to back of map.
func (b *Map[K, V]) PushBack(key K, value V) bool {
	b.init()
	return b.tree.PushBack(key, value)
}

// PopFront removes first item.
func (b *Map[K, V]) PopFront() (K, V, bool) {
	return b.tree.PopFront()
}

// PopBack removes last item.
func (b *Map[K, V]) PopBack() (K, V, bool) {
	return b.tree.PopBack()
}

// PopFrontFunc removes first item if provided function allows.
func (b *Map[K, V]) PopFrontFunc(cond func(key K, value V) bool) (K, V, bool) {
	return b.tree.PopFrontFunc(cond)
}

// PopBackFunc removes last item if provided function allows.
func (b *Map[K, V]) PopBackFunc(cond func(key K, value V) bool) (K, V, bool) {
	return b.tree.PopBackFunc(cond)
}

// Front returns first item
func (b *Map[K, V]) Front() (K, V, bool) {
	return b.tree.Front()
}

// Back returns first item
func (b *Map[K, V]) Back() (K, V, bool) {
	return b.tree.Back()
}

// Front returns first item
func (b *Map[K, V]) FrontMut() (K, V, bool) {
	return b.tree.FrontMut()
}

// BackMut returns first item
// MUTABLE OPERATION
func (b *Map[K, V]) BackMut() (K, V, bool) {
	return b.tree.BackMut()
}

// InsertAt inserts item at index
func (b *Map[K, V]) InsertAt(index int, key K, value V) bool {
	b.init()
	return b.tree.InsertAt(index, key, value)
}

// DeleteAt deletes item at index
func (b *Map[K, V]) DeleteAt(index int) (K, V, bool) {
	return b.tree.DeleteAt(index)
}

// ReplaceAt replaces item at index
func (b *Map[K, V]) ReplaceAt(index int, key K, value V) (K, V, bool) {
	return b.tree.ReplaceAt(index, key, value)
}

// LookupAt returns item at index
func (b *Map[K, V]) LookupAt(index int) (K, V, bool) {
	return b.tree.LookupAt(index)
}

// LookupAtMut returns item at index
// MUTABLE OPERATION
func (b *Map[K, V]) LookupAtMut(index int) (K, V, bool) {
	return b.tree.LookupAtMut(index)
}

// GetAt returns item at index
func (b *Map[K, V]) GetAt(index int) (K, V) {
	return b.tree.GetAt(index)
}

// GetAtMut returns item at index
// MUTABLE OPERATION
func (b *Map[K, V]) GetAtMut(index int) (K, V) {
	return b.tree.GetAtMut(index)
}

// Keys returns an iterator of all keys.
func (b *Map[K, V]) Keys() iter.Seq[K] {
	return iterK(b.tree.All())
}

// Values returns an iterator of all values.
func (b *Map[K, V]) Values() iter.Seq[V] {
	return iterV(b.tree.All())
}

// ValuesMut returns an iterator of all values.
// MUTABLE OPERATION
func (b *Map[K, V]) ValuesMut() iter.Seq[V] {
	return iterV(b.tree.AllMut())
}

// Seek returns the first item that is >= key
func (b *Map[K, V]) Seek(key K) (K, V, bool) {
	var emptyValue V
	return b.tree.Seek(key, emptyValue)
}

// SeekMut returns the first item that is >= key
// MUTABLE OPERATION
func (b *Map[K, V]) SeekMut(key K) (K, V, bool) {
	var emptyValue V
	return b.tree.SeekMut(key, emptyValue)
}

// SeekNext returns the first item that is > key
func (b *Map[K, V]) SeekNext(key K) (K, V, bool) {
	var emptyValue V
	return b.tree.SeekNext(key, emptyValue)
}

// SeekNextMut returns the first item that is > key
// MUTABLE OPERATION
func (b *Map[K, V]) SeekNextMut(key K) (K, V, bool) {
	var emptyValue V
	return b.tree.SeekNextMut(key, emptyValue)
}

// SeekPrev returns the first item that is < key
func (b *Map[K, V]) SeekPrev(key K) (K, V, bool) {
	var emptyValue V
	return b.tree.SeekPrev(key, emptyValue)
}

// SeekPrevMut returns the first item that is < key
// MUTABLE OPERATION
func (b *Map[K, V]) SeekPrevMut(key K) (K, V, bool) {
	var emptyValue V
	return b.tree.SeekPrevMut(key, emptyValue)
}

// DrainOLD deletes all items from map and returns them as an iterator
func (b *Map[K, V]) DrainOLD() iter.Seq2[K, V] {
	return b.tree.DrainOLD()
}

// DrainBackwardOLD deletes all items from map and returns them as an iterator, in
// reverse order
func (b *Map[K, V]) DrainBackwardOLD() iter.Seq2[K, V] {
	return b.tree.DrainBackwardOLD()
}

// String returns a string representation of map
func (b *Map[K, V]) String() string {
	var s []byte
	s = append(s, "map["...)
	var i int
	for k, v := range b.All() {
		if i > 0 {
			s = append(s, ' ')
		}
		s = fmt.Appendf(s, "%v:%v", k, v)
		i++
	}
	s = append(s, "]"...)
	return string(s)
}

// DeleteFunc delete item from map if its allowed by del function.
func (b *Map[K, V]) DeleteFunc(del func(K, V) bool) bool {
	var ok bool
	var index int
more:
	for k, v := range b.AscendAt(index) {
		if del(k, v) {
			_, _, ok = b.DeleteAt(index)
			goto more
		}
		index++
	}
	return ok
}
