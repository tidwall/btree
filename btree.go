// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
package btree

import (
	"sync"
)

const maxItems = 255 // max items per node. max children is +1
const minItems = maxItems * 40 / 100

type cow struct {
	_ int // it cannot be an empty struct
}

type node struct {
	cow      *cow
	count    int
	items    []interface{}
	children *[]*node
}

type BTree struct {
	mu    *sync.RWMutex
	cow   *cow
	root  *node
	count int
	less  func(a, b interface{}) bool
	locks bool
}

func (tr *BTree) newNode(leaf bool) *node {
	n := &node{cow: tr.cow}
	if !leaf {
		n.children = new([]*node)
	}
	return n
}

// leaf returns true if the node is a leaf.
func (n *node) leaf() bool {
	return n.children == nil
}

// PathHint is a utility type used with the *Hint() functions. Hints provide
// faster operations for clustered keys.
type PathHint struct {
	used [8]bool
	path [8]uint8
}

// New returns a new BTree
func New(less func(a, b interface{}) bool) *BTree {
	return newBTree(less, true)
}

// NewNonConcurrent returns a new BTree which is not safe for concurrent
// write operations by multiple goroutines.
//
// This is useful for when you do not need the BTree to manage the locking,
// but would rather do it yourself.
func NewNonConcurrent(less func(a, b interface{}) bool) *BTree {
	return newBTree(less, false)
}

func newBTree(less func(a, b interface{}) bool, locks bool) *BTree {
	if less == nil {
		panic("nil less")
	}
	tr := new(BTree)
	tr.cow = new(cow)
	tr.mu = new(sync.RWMutex)
	tr.less = less
	tr.locks = locks
	return tr
}

// Less is a convenience function that performs a comparison of two items
// using the same "less" function provided to New.
func (tr *BTree) Less(a, b interface{}) bool {
	return tr.less(a, b)
}

func (tr *BTree) find(n *node, key interface{},
	hint *PathHint, depth int,
) (index int, found bool) {
	if hint == nil {
		// fast path for no hinting
		low := 0
		high := len(n.items)
		for low < high {
			mid := (low + high) / 2
			if !tr.Less(key, n.items[mid]) {
				low = mid + 1
			} else {
				high = mid
			}
		}
		if low > 0 && !tr.less(n.items[low-1], key) {
			return low - 1, true
		}
		return low, false
	}

	// Try using hint.
	// Best case finds the exact match, updates the hint and returns.
	// Worst case, updates the low and high bounds to binary search between.
	low := 0
	high := len(n.items) - 1
	if depth < 8 && hint.used[depth] {
		index = int(hint.path[depth])
		if index >= len(n.items) {
			// tail item
			if tr.less(n.items[len(n.items)-1], key) {
				index = len(n.items)
				goto path_match
			}
			index = len(n.items) - 1
		}
		if tr.less(key, n.items[index]) {
			if index == 0 || tr.less(n.items[index-1], key) {
				goto path_match
			}
			high = index - 1
		} else if tr.less(n.items[index], key) {
			low = index + 1
		} else {
			found = true
			goto path_match
		}
	}

	// Do a binary search between low and high
	// keep on going until low > high, where the guarantee on low is that
	// key >= items[low - 1]
	for low <= high {
		mid := low + ((high+1)-low)/2
		// if key >= n.items[mid], low = mid + 1
		// which implies that key >= everything below low
		if !tr.less(key, n.items[mid]) {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	// if low > 0, n.items[low - 1] >= key,
	// we have from before that key >= n.items[low - 1]
	// therefore key = n.items[low - 1],
	// and we have found the entry for key.
	// Otherwise we must keep searching for the key in index `low`.
	if low > 0 && !tr.less(n.items[low-1], key) {
		index = low - 1
		found = true
	} else {
		index = low
		found = false
	}

path_match:
	if depth < 8 {
		hint.used[depth] = true
		var pathIndex uint8
		if n.leaf() && found {
			pathIndex = uint8(index + 1)
		} else {
			pathIndex = uint8(index)
		}
		if pathIndex != hint.path[depth] {
			hint.path[depth] = pathIndex
			for i := depth + 1; i < 8; i++ {
				hint.used[i] = false
			}
		}
	}
	return index, found
}

// SetHint sets or replace a value for a key using a path hint
func (tr *BTree) SetHint(item interface{}, hint *PathHint) (prev interface{}) {
	if tr.lock() {
		defer tr.unlock()
	}
	return tr.setHint(item, hint)
}

func (tr *BTree) setHint(item interface{}, hint *PathHint) (prev interface{}) {
	if item == nil {
		panic("nil item")
	}
	if tr.root == nil {
		tr.root = tr.newNode(true)
		tr.root.items = append([]interface{}{}, item)
		tr.root.count = 1
		tr.count = 1
		return nil
	}
	prev, split := tr.nodeSet(&tr.root, item, hint, 0)
	if split {
		left := tr.cowLoad(&tr.root)
		right, median := tr.nodeSplit(left)
		tr.root = tr.newNode(false)
		*tr.root.children = make([]*node, 0, maxItems+1)
		*tr.root.children = append([]*node{}, left, right)
		tr.root.items = append([]interface{}{}, median)
		tr.root.updateCount()
		return tr.setHint(item, hint)
	}
	if prev != nil {
		return prev
	}
	tr.count++
	return nil
}

// Set or replace a value for a key
func (tr *BTree) Set(item interface{}) interface{} {
	return tr.SetHint(item, nil)
}

func (tr *BTree) nodeSplit(n *node) (right *node, median interface{}) {
	i := maxItems / 2
	median = n.items[i]

	// left node
	left := tr.newNode(n.leaf())
	left.items = make([]interface{}, len(n.items[:i]), maxItems/2)
	copy(left.items, n.items[:i])
	if !n.leaf() {
		*left.children = make([]*node, len((*n.children)[:i+1]), maxItems+1)
		copy(*left.children, (*n.children)[:i+1])
	}
	left.updateCount()

	// right node
	right = tr.newNode(n.leaf())
	right.items = make([]interface{}, len(n.items[i+1:]), maxItems/2)
	copy(right.items, n.items[i+1:])
	if !n.leaf() {
		*right.children = make([]*node, len((*n.children)[i+1:]), maxItems+1)
		copy(*right.children, (*n.children)[i+1:])
	}
	right.updateCount()

	*n = *left
	return right, median
}

func (n *node) updateCount() {
	n.count = len(n.items)
	if !n.leaf() {
		for i := 0; i < len(*n.children); i++ {
			n.count += (*n.children)[i].count
		}
	}
}

// This operation should not be inlined because it's expensive and rarely
// called outside of heavy copy-on-write situations. Marking it "noinline"
// allows for the parent cowLoad to be inlined.
// go:noinline
func (tr *BTree) copy(n *node) *node {
	n2 := new(node)
	n2.cow = tr.cow
	n2.count = n.count
	n2.items = make([]interface{}, len(n.items), cap(n.items))
	copy(n2.items, n.items)
	if !n.leaf() {
		n2.children = new([]*node)
		*n2.children = make([]*node, len(*n.children), maxItems+1)
		copy(*n2.children, *n.children)
	}
	return n2
}

// cowLoad loads the provided node and, if needed, performs a copy-on-write.
func (tr *BTree) cowLoad(cn **node) *node {
	if (*cn).cow != tr.cow {
		*cn = tr.copy(*cn)
	}
	return *cn
}

func (tr *BTree) nodeSet(cn **node, item interface{},
	hint *PathHint, depth int,
) (prev interface{}, split bool) {
	n := tr.cowLoad(cn)
	i, found := tr.find(n, item, hint, depth)
	if found {
		prev = n.items[i]
		n.items[i] = item
		return prev, false
	}
	if n.leaf() {
		if len(n.items) == maxItems {
			return nil, true
		}
		n.items = append(n.items, nil)
		copy(n.items[i+1:], n.items[i:])
		n.items[i] = item
		n.count++
		return nil, false
	}
	prev, split = tr.nodeSet(&(*n.children)[i], item, hint, depth+1)
	if split {
		if len(n.items) == maxItems {
			return nil, true
		}
		right, median := tr.nodeSplit((*n.children)[i])
		*n.children = append(*n.children, nil)
		copy((*n.children)[i+1:], (*n.children)[i:])
		(*n.children)[i+1] = right
		n.items = append(n.items, nil)
		copy(n.items[i+1:], n.items[i:])
		n.items[i] = median
		return tr.nodeSet(&n, item, hint, depth)
	}
	if prev == nil {
		n.count++
	}
	return prev, false
}

func (n *node) scan(iter func(item interface{}) bool) bool {
	if n.leaf() {
		for i := 0; i < len(n.items); i++ {
			if !iter(n.items[i]) {
				return false
			}
		}
		return true
	}
	for i := 0; i < len(n.items); i++ {
		if !(*n.children)[i].scan(iter) {
			return false
		}
		if !iter(n.items[i]) {
			return false
		}
	}
	return (*n.children)[len(*n.children)-1].scan(iter)
}

// Get a value for key
func (tr *BTree) Get(key interface{}) interface{} {
	return tr.GetHint(key, nil)
}

// GetHint gets a value for key using a path hint
func (tr *BTree) GetHint(key interface{}, hint *PathHint) interface{} {
	if tr.rlock() {
		defer tr.runlock()
	}
	if tr.root == nil {
		return nil
	}
	n := tr.root
	depth := 0
	for {
		i, found := tr.find(n, key, hint, depth)
		if found {
			return n.items[i]
		}
		if n.children == nil {
			return nil
		}
		n = (*n.children)[i]
		depth++
	}
}

// Len returns the number of items in the tree
func (tr *BTree) Len() int {
	return tr.count
}

// Delete a value for a key
func (tr *BTree) Delete(key interface{}) interface{} {
	return tr.DeleteHint(key, nil)
}

// DeleteHint deletes a value for a key using a path hint
func (tr *BTree) DeleteHint(key interface{}, hint *PathHint) interface{} {
	if tr.lock() {
		defer tr.unlock()
	}
	return tr.deleteHint(key, hint)
}

func (tr *BTree) deleteHint(key interface{}, hint *PathHint) interface{} {
	if tr.root == nil || key == nil {
		return nil
	}
	prev := tr.delete(&tr.root, false, key, tr.less, hint, 0)
	if prev == nil {
		return nil
	}
	if len(tr.root.items) == 0 && !tr.root.leaf() {
		tr.root = (*tr.root.children)[0]
	}
	tr.count--
	if tr.count == 0 {
		tr.root = nil
	}
	return prev
}

func (tr *BTree) delete(cn **node, max bool, key interface{},
	less func(a, b interface{}) bool, hint *PathHint, depth int,
) interface{} {
	n := tr.cowLoad(cn)
	var i int
	var found bool
	if max {
		i, found = len(n.items)-1, true
	} else {
		i, found = tr.find(n, key, hint, depth)
	}
	if n.leaf() {
		if found {
			// found the items at the leaf, remove it and return.
			prev := n.items[i]
			copy(n.items[i:], n.items[i+1:])
			n.items[len(n.items)-1] = nil
			n.items = n.items[:len(n.items)-1]
			n.count--
			return prev
		}
		return nil
	}

	var prev interface{}
	if found {
		if max {
			i++
			prev = tr.delete(&(*n.children)[i], true, "", less, nil, 0)
		} else {
			prev = n.items[i]
			maxItem := tr.delete(&(*n.children)[i], true, "", less, nil, 0)
			n.items[i] = maxItem
		}
	} else {
		prev = tr.delete(&(*n.children)[i], max, key, less, hint, depth+1)
	}
	if prev == nil {
		return nil
	}
	n.count--
	if len((*n.children)[i].items) < minItems {
		tr.nodeRebalance(n, i)
	}
	return prev

}

// nodeRebalance rebalances the child nodes following a delete operation.
// Provide the index of the child node with the number of items that fell
// below minItems.
func (tr *BTree) nodeRebalance(n *node, i int) {
	if i == len(n.items) {
		i--
	}

	// ensure copy-on-write
	left := tr.cowLoad(&(*n.children)[i])
	right := tr.cowLoad(&(*n.children)[i+1])

	if len(left.items)+len(right.items) < maxItems {
		// Merges the left and right children nodes together as a single node
		// that includes (left,item,right), and places the contents into the
		// existing left node. Delete the right node altogether and move the
		// following items and child nodes to the left by one slot.

		// merge (left,item,right)
		left.items = append(left.items, n.items[i])
		left.items = append(left.items, right.items...)
		if !left.leaf() {
			*left.children = append(*left.children, *right.children...)
		}
		left.count += right.count + 1

		// move the items over one slot
		copy(n.items[i:], n.items[i+1:])
		n.items[len(n.items)-1] = nil
		n.items = n.items[:len(n.items)-1]

		// move the children over one slot
		copy((*n.children)[i+1:], (*n.children)[i+2:])
		(*n.children)[len(*n.children)-1] = nil
		(*n.children) = (*n.children)[:len(*n.children)-1]
	} else if len(left.items) > len(right.items) {
		// move left -> right over one slot

		// Move the item of the parent node at index into the right-node first
		// slot, and move the left-node last item into the previously moved
		// parent item slot.
		right.items = append(right.items, nil)
		copy(right.items[1:], right.items)
		right.items[0] = n.items[i]
		right.count++
		n.items[i] = left.items[len(left.items)-1]
		left.items[len(left.items)-1] = nil
		left.items = left.items[:len(left.items)-1]
		left.count--

		if !left.leaf() {
			// move the left-node last child into the right-node first slot
			*right.children = append(*right.children, nil)
			copy((*right.children)[1:], *right.children)
			(*right.children)[0] = (*left.children)[len(*left.children)-1]
			(*left.children)[len(*left.children)-1] = nil
			(*left.children) = (*left.children)[:len(*left.children)-1]
			left.count -= (*right.children)[0].count
			right.count += (*right.children)[0].count
		}
	} else {
		// move left <- right over one slot

		// Same as above but the other direction
		left.items = append(left.items, n.items[i])
		left.count++
		n.items[i] = right.items[0]
		copy(right.items, right.items[1:])
		right.items[len(right.items)-1] = nil
		right.items = right.items[:len(right.items)-1]
		right.count--

		if !left.leaf() {
			*left.children = append(*left.children, (*right.children)[0])
			copy(*right.children, (*right.children)[1:])
			(*right.children)[len(*right.children)-1] = nil
			*right.children = (*right.children)[:len(*right.children)-1]
			left.count += (*left.children)[len(*left.children)-1].count
			right.count -= (*left.children)[len(*left.children)-1].count
		}
	}
}

// Ascend the tree within the range [pivot, last]
// Pass nil for pivot to scan all item in ascending order
// Return false to stop iterating
func (tr *BTree) Ascend(pivot interface{}, iter func(item interface{}) bool) {
	if tr.rlock() {
		defer tr.runlock()
	}
	if tr.root == nil {
		return
	}
	if pivot == nil {
		tr.root.scan(iter)
	} else if tr.root != nil {
		tr.ascend(tr.root, pivot, nil, 0, iter)
	}
}

// The return value of this function determines whether we should keep iterating
// upon this functions return.
func (tr *BTree) ascend(n *node, pivot interface{},
	hint *PathHint, depth int, iter func(item interface{}) bool,
) bool {
	i, found := tr.find(n, pivot, hint, depth)
	if !found {
		if !n.leaf() {
			if !tr.ascend((*n.children)[i], pivot, hint, depth+1, iter) {
				return false
			}
		}
	}
	// We are either in the case that
	// - node is found, we should iterate through it starting at `i`,
	//   the index it was located at.
	// - node is not found, and TODO: fill in.
	for ; i < len(n.items); i++ {
		if !iter(n.items[i]) {
			return false
		}
		if !n.leaf() {
			if !(*n.children)[i+1].scan(iter) {
				return false
			}
		}
	}
	return true
}

func (n *node) reverse(iter func(item interface{}) bool) bool {
	if n.leaf() {
		for i := len(n.items) - 1; i >= 0; i-- {
			if !iter(n.items[i]) {
				return false
			}
		}
		return true
	}
	if !(*n.children)[len(*n.children)-1].reverse(iter) {
		return false
	}
	for i := len(n.items) - 1; i >= 0; i-- {
		if !iter(n.items[i]) {
			return false
		}
		if !(*n.children)[i].reverse(iter) {
			return false
		}
	}
	return true
}

// Descend the tree within the range [pivot, first]
// Pass nil for pivot to scan all item in descending order
// Return false to stop iterating
func (tr *BTree) Descend(pivot interface{}, iter func(item interface{}) bool) {
	if tr.rlock() {
		defer tr.runlock()
	}
	if tr.root == nil {
		return
	}
	if pivot == nil {
		tr.root.reverse(iter)
	} else if tr.root != nil {
		tr.descend(tr.root, pivot, nil, 0, iter)
	}
}

func (tr *BTree) descend(n *node, pivot interface{},
	hint *PathHint, depth int, iter func(item interface{}) bool,
) bool {
	i, found := tr.find(n, pivot, hint, depth)
	if !found {
		if !n.leaf() {
			if !tr.descend((*n.children)[i], pivot, hint, depth+1, iter) {
				return false
			}
		}
		i--
	}
	for ; i >= 0; i-- {
		if !iter(n.items[i]) {
			return false
		}
		if !n.leaf() {
			if !(*n.children)[i].reverse(iter) {
				return false
			}
		}
	}
	return true
}

// Load is for bulk loading pre-sorted items
func (tr *BTree) Load(item interface{}) interface{} {
	if item == nil {
		panic("nil item")
	}
	if tr.lock() {
		defer tr.unlock()
	}
	if tr.root == nil {
		return tr.setHint(item, nil)
	}
	n := tr.cowLoad(&tr.root)
	for {
		n.count++ // optimistically update counts
		if n.leaf() {
			if len(n.items) < maxItems-1 {
				if tr.less(n.items[len(n.items)-1], item) {
					n.items = append(n.items, item)
					tr.count++
					return nil
				}
			}
			break
		}
		n = tr.cowLoad(&(*n.children)[len(*n.children)-1])
	}
	// revert the counts
	n = tr.root
	for {
		n.count--
		if n.leaf() {
			break
		}
		n = (*n.children)[len(*n.children)-1]
	}
	return tr.setHint(item, nil)
}

// Min returns the minimum item in tree.
// Returns nil if the tree has no items.
func (tr *BTree) Min() interface{} {
	if tr.rlock() {
		defer tr.runlock()
	}
	if tr.root == nil {
		return nil
	}
	n := tr.root
	for {
		if n.leaf() {
			return n.items[0]
		}
		n = (*n.children)[0]
	}
}

// Max returns the maximum item in tree.
// Returns nil if the tree has no items.
func (tr *BTree) Max() interface{} {
	if tr.rlock() {
		defer tr.runlock()
	}
	if tr.root == nil {
		return nil
	}
	n := tr.root
	for {
		if n.leaf() {
			return n.items[len(n.items)-1]
		}
		n = (*n.children)[len(*n.children)-1]
	}
}

// PopMin removes the minimum item in tree and returns it.
// Returns nil if the tree has no items.
func (tr *BTree) PopMin() interface{} {
	if tr.lock() {
		defer tr.unlock()
	}
	if tr.root == nil {
		return nil
	}
	n := tr.cowLoad(&tr.root)
	var item interface{}
	for {
		n.count-- // optimistically update counts
		if n.leaf() {
			item = n.items[0]
			if len(n.items) == minItems {
				break
			}
			copy(n.items[:], n.items[1:])
			n.items[len(n.items)-1] = nil
			n.items = n.items[:len(n.items)-1]
			tr.count--
			if tr.count == 0 {
				tr.root = nil
			}
			return item
		}
		n = tr.cowLoad(&(*n.children)[0])
	}
	// revert the counts
	n = tr.root
	for {
		n.count++
		if n.leaf() {
			break
		}
		n = (*n.children)[0]
	}
	return tr.deleteHint(item, nil)
}

// PopMax removes the minimum item in tree and returns it.
// Returns nil if the tree has no items.
func (tr *BTree) PopMax() interface{} {
	if tr.lock() {
		defer tr.unlock()
	}
	if tr.root == nil {
		return nil
	}
	n := tr.cowLoad(&tr.root)
	var item interface{}
	for {
		n.count-- // optimistically update counts
		if n.leaf() {
			item = n.items[len(n.items)-1]
			if len(n.items) == minItems {
				break
			}
			n.items[len(n.items)-1] = nil
			n.items = n.items[:len(n.items)-1]
			tr.count--
			if tr.count == 0 {
				tr.root = nil
			}
			return item
		}
		n = tr.cowLoad(&(*n.children)[len(*n.children)-1])
	}
	// revert the counts
	n = tr.root
	for {
		n.count++
		if n.leaf() {
			break
		}
		n = (*n.children)[len(*n.children)-1]
	}
	return tr.deleteHint(item, nil)
}

// GetAt returns the value at index.
// Return nil if the tree is empty or the index is out of bounds.
func (tr *BTree) GetAt(index int) interface{} {
	if tr.rlock() {
		defer tr.runlock()
	}
	if tr.root == nil || index < 0 || index >= tr.count {
		return nil
	}
	n := tr.root
	for {
		if n.leaf() {
			return n.items[index]
		}
		i := 0
		for ; i < len(n.items); i++ {
			if index < (*n.children)[i].count {
				break
			} else if index == (*n.children)[i].count {
				return n.items[i]
			}
			index -= (*n.children)[i].count + 1
		}
		n = (*n.children)[i]
	}
}

// DeleteAt deletes the item at index.
// Return nil if the tree is empty or the index is out of bounds.
func (tr *BTree) DeleteAt(index int) interface{} {
	if tr.lock() {
		defer tr.unlock()
	}
	if tr.root == nil || index < 0 || index >= tr.count {
		return nil
	}
	var pathbuf [8]uint8 // track the path
	path := pathbuf[:0]
	var item interface{}
	n := tr.cowLoad(&tr.root)
outer:
	for {
		n.count-- // optimistically update counts
		if n.leaf() {
			// the index is the item position
			item = n.items[index]
			if len(n.items) == minItems {
				path = append(path, uint8(index))
				break outer
			}
			copy(n.items[index:], n.items[index+1:])
			n.items[len(n.items)-1] = nil
			n.items = n.items[:len(n.items)-1]
			tr.count--
			if tr.count == 0 {
				tr.root = nil
			}
			return item
		}
		i := 0
		for ; i < len(n.items); i++ {
			if index < (*n.children)[i].count {
				break
			} else if index == (*n.children)[i].count {
				item = n.items[i]
				path = append(path, uint8(i))
				break outer
			}
			index -= (*n.children)[i].count + 1
		}
		path = append(path, uint8(i))
		n = tr.cowLoad(&(*n.children)[i])
	}
	// revert the counts
	var hint PathHint
	n = tr.root
	for i := 0; i < len(path); i++ {
		if i < len(hint.path) {
			hint.path[i] = uint8(path[i])
			hint.used[i] = true
		}
		n.count++
		if !n.leaf() {
			n = (*n.children)[uint8(path[i])]
		}
	}
	return tr.deleteHint(item, &hint)
}

// Height returns the height of the tree.
// Returns zero if tree has no items.
func (tr *BTree) Height() int {
	if tr.rlock() {
		defer tr.runlock()
	}
	var height int
	if tr.root != nil {
		n := tr.root
		for {
			height++
			if n.leaf() {
				break
			}
			n = (*n.children)[0]
		}
	}
	return height
}

// Walk iterates over all items in tree, in order.
// The items param will contain one or more items.
func (tr *BTree) Walk(iter func(item []interface{})) {
	if tr.rlock() {
		defer tr.runlock()
	}
	if tr.root != nil {
		tr.root.walk(iter)
	}
}

func (n *node) walk(iter func(item []interface{})) {
	if n.leaf() {
		iter(n.items)
	} else {
		for i := 0; i < len(n.items); i++ {
			(*n.children)[i].walk(iter)
			iter(n.items[i : i+1])
		}
		(*n.children)[len(n.items)].walk(iter)
	}
}

// Copy the tree. This is a copy-on-write operation and is very fast because
// it only performs a shadowed copy.
func (tr *BTree) Copy() *BTree {
	if tr.lock() {
		defer tr.unlock()
	}
	tr.cow = new(cow)
	tr2 := new(BTree)
	*tr2 = *tr
	tr2.mu = new(sync.RWMutex)
	tr2.cow = new(cow)
	return tr2
}

func (tr *BTree) lock() bool {
	if tr.locks {
		tr.mu.Lock()
	}
	return tr.locks
}

func (tr *BTree) unlock() {
	tr.mu.Unlock()
}

func (tr *BTree) rlock() bool {
	if tr.locks {
		tr.mu.RLock()
	}
	return tr.locks
}

func (tr *BTree) runlock() {
	tr.mu.RUnlock()
}
