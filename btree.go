// https://github.com/tidwall/btree
//
// Copyright 2026 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
//
// B-tree based collections for go
package btree // import "github.com/tidwall/btree/v2"

import (
	"iter"
	"sync/atomic"
	"unsafe"
)

const fanout = 64
const maxItems = fanout - 1
const minItems = maxItems / 2

type tree[K, V any] struct {
	cloned      bool        // Clone() called at least once during life of tree.
	strprefix   bool        // Use string prefixes, K _MUST_ be a string.
	nopre       bool        // flag used by outer types
	stdops      bool        // flag used by outer types
	alt         bool        // flag used by outer types
	initd       bool        // flag used by outer types
	count       int         // total tree count
	root        *node[K, V] // root node
	dataCopy    func(V) V   // Value data copy-on-write
	dataRelease func(V)     // Value data on release
	dataCompare func(K, V, K, V) int
	dataSearch  func(int, *K, *V, K, V) (int, bool)
}

type node[K, V any] struct {
	keys   [maxItems]K   // item keys, _MUST_ be first field
	values [maxItems]V   // item values
	rc     atomic.Int64  // copy-on-write reference counter
	len    int           // number of items (key value pairs)
	branch *branch[K, V] // branch children (nil for leaf)
}

type branch[K, V any] struct {
	children [maxItems + 1]*node[K, V] // all child nodes
	counts   [maxItems + 1]int         // all child counts
	prefixes *[maxItems]uint64         // string prefixes
}

type branchAlloc[K, V any] struct {
	node   node[K, V]
	branch branch[K, V]
}

type branchPrefixAlloc[K, V any] struct {
	node     node[K, V]
	branch   branch[K, V]
	prefixes [maxItems]uint64
}

type omit struct{}

func (t *tree[K, V]) newNode(leaf bool) *node[K, V] {
	var n *node[K, V]
	if leaf {
		n = new(node[K, V])
	} else if t.strprefix {
		b := new(branchPrefixAlloc[K, V])
		b.node.branch = &b.branch
		b.node.branch.prefixes = &b.prefixes
		n = (*node[K, V])(unsafe.Pointer(b))
	} else {
		b := new(branchAlloc[K, V])
		b.node.branch = &b.branch
		n = (*node[K, V])(unsafe.Pointer(b))
	}
	return n
}

func (n *node[K, V]) leaf() bool {
	return n.branch == nil
}

// Return the total number of items in node/subtree.
func (n *node[K, V]) count() int {
	var count int
	if n != nil {
		count = n.len
		if !n.leaf() {
			counts := n.branch.counts[:n.len+1]
			for _, c := range counts {
				count += c
			}
		}
	}
	return count
}

func (t *tree[K, V]) splitNode(n *node[K, V]) (*node[K, V], K, V) {
	m := maxItems - minItems - 1
	mkey := n.keys[m]
	mvalue := n.values[m]
	right := t.newNode(n.leaf())
	n.len = m
	right.len = maxItems - m - 1
	copy(right.keys[:], n.keys[m+1:])
	copy(right.values[:], n.values[m+1:])
	var emptyKey K
	var emptyValue V
	for i := n.len; i < maxItems; i++ {
		n.keys[i] = emptyKey
	}
	for i := n.len; i < maxItems; i++ {
		n.values[i] = emptyValue
	}
	if !n.leaf() {
		copy(right.branch.children[:], n.branch.children[m+1:])
		for i := n.len + 1; i <= maxItems; i++ {
			n.branch.children[i] = nil
		}
		copy(right.branch.counts[:], n.branch.counts[m+1:])
		if n.branch.prefixes != nil {
			copy(right.branch.prefixes[:], n.branch.prefixes[m+1:])
		}
	}
	return right, mkey, mvalue
}

//go:noinline
func (t *tree[K, V]) splitBranch(n *node[K, V], i int) {
	right, mkey, mvalue := t.splitNode(n.branch.children[i])
	t.insertBranchItemAt(n, mkey, mvalue, i)
	n.insertChildAt(right, i+1)
	n.branch.counts[i] = n.branch.children[i].count()
	n.branch.counts[i+1] = n.branch.children[i+1].count()
}

// ensure child branch has space to include a new item.
// Return 1 if split, or 0 if no change
func (t *tree[K, V]) ensureBranch(n *node[K, V], i int) bool {
	if n.branch.children[i].len == maxItems {
		t.splitBranch(n, i)
		return true
	}
	return false
}

func (t *tree[K, V]) splitRoot() {
	root2 := t.newNode(false)
	right, mkey, mvalue := t.splitNode(t.root)
	root2.branch.children[0] = t.root
	root2.branch.children[1] = right
	t.insertBranchItemAt(root2, mkey, mvalue, 0)
	t.root = root2
	root2.branch.counts[0] = root2.branch.children[0].count()
	root2.branch.counts[1] = root2.branch.children[1].count()
}

// ensure root node has space to include a new item. Return true if split.
func (t *tree[K, V]) ensureRoot() (wasSplit bool) {
	if t.root.len == maxItems {
		t.splitRoot()
		return true
	}
	return false
}

func (t *tree[K, V]) insertBranchItemAt(n *node[K, V], key K, value V, i int,
) {
	if n.branch.prefixes != nil {
		copy(n.branch.prefixes[i+1:], n.branch.prefixes[i:n.len])
		n.branch.prefixes[i] = prefixString(key)
	}
	n.insertItemAt(key, value, i)
}

func (n *node[K, V]) insertItemAt(key K, value V, i int) {
	copy(n.keys[i+1:], n.keys[i:n.len])
	n.keys[i] = key
	copy(n.values[i+1:], n.values[i:n.len])
	n.values[i] = value
	n.len++
}

func (n *node[K, V]) insertChildAt(child *node[K, V], i int) {
	// n.len was already incremented by caller
	copy(n.branch.children[i+1:], n.branch.children[i:n.len])
	n.branch.children[i] = child
	copy(n.branch.counts[i+1:], n.branch.counts[i:n.len])
}

// Insert the first item into a map. This makes the root a new leaf and set the
// count to 1.
func (t *tree[K, V]) insertFirstItem(key K, value V) {
	t.root = t.newNode(true)
	t.root.keys[0] = key
	t.root.values[0] = value
	t.root.len = 1
	t.count = 1
}

func (t *tree[K, V]) releaseNode(n *node[K, V]) {
	if n.rc.Add(-1) < 0 {
		// Release items and children
		if t.dataRelease != nil {
			for i := 0; i < n.len; i++ {
				t.dataRelease(n.values[i])
			}
		}
		if !n.leaf() {
			for i := 0; i <= n.len; i++ {
				t.releaseNode(n.branch.children[i])
			}
		}
	}
}

func (t *tree[K, V]) copyNode(n *node[K, V]) *node[K, V] {
	n2 := t.newNode(n.leaf())
	n2.len = n.len
	copy(n2.keys[:], n.keys[:n.len])
	if t != nil && t.dataCopy != nil {
		// Perform user-defined value copy on each value.
		for i := 0; i < n.len; i++ {
			n2.values[i] = t.dataCopy(n.values[i])
		}
	} else {
		copy(n2.values[:], n.values[:n.len])
	}
	if !n.leaf() {
		for i := 0; i < n.len+1; i++ {
			n.branch.children[i].rc.Add(1)
			n2.branch.children[i] = n.branch.children[i]
		}
		copy(n2.branch.counts[:], n.branch.counts[:n.len+1])
		if n.branch.prefixes != nil {
			copy(n2.branch.prefixes[:], n.branch.prefixes[:n.len])
		}
	}
	return n2
}

// Performs the actual copy-on-write for provided node.
// This _must_ only be called from the t.cowRoot or t.cowChild functions.
// Do not call directly.
//
//go:noinline
func (t *tree[K, V]) cow0(pn **node[K, V]) {
	n2 := t.copyNode(*pn)
	t.releaseNode(*pn)
	*pn = n2
}

// should inline
func (t *tree[K, V]) cowRoot(mut bool) {
	if mut && t.cloned && t.root != nil && t.root.rc.Load() > 0 {
		t.cow0(&t.root)
	}
}

// should inline
func (t *tree[K, V]) cowChild(n *node[K, V], i int, mut bool) {
	if mut && t.cloned && n.branch.children[i].rc.Load() > 0 {
		t.cow0(&n.branch.children[i])
	}
}

func (t *tree[K, V]) nodeDeleteMax(n *node[K, V]) (K, V) {
	var emptyKey K
	var emptyValue V
	i := n.len - 1
	if n.leaf() {
		oldKey := n.keys[i]
		oldValue := n.values[i]
		copy(n.keys[i:n.len-1], n.keys[i+1:n.len])
		n.keys[n.len-1] = emptyKey
		copy(n.values[i:n.len-1], n.values[i+1:n.len])
		n.values[n.len-1] = emptyValue
		n.len--
		return oldKey, oldValue
	}
	i++
	t.cowChild(n, i, true)
	oldKey, oldValue := t.nodeDeleteMax(n.branch.children[i])
	n.branch.counts[i]--
	if n.branch.children[i].len < minItems {
		t.rebalance(n, i)
	}
	return oldKey, oldValue
}

// Rebalance the child nodes following a delete operation.
// Provide the index of the child node that has fallen below minItems.
func (t *tree[K, V]) rebalance(n *node[K, V], i int) {
	var emptyKey K
	var emptyValue V
	if i == n.len {
		i--
	}

	// Ensure copy-on-write
	t.cowChild(n, i, true)
	t.cowChild(n, i+1, true)

	left := n.branch.children[i]
	right := n.branch.children[i+1]

	if left.len+right.len < maxItems {
		// Merges the left and right children nodes together as a single node
		// that includes (left,item,right), and places the contents into the
		// existing left node. Delete the right node altogether and move the
		// following items and child nodes to the left by one slot.

		// merge (left,item,right)
		left.keys[left.len] = n.keys[i]
		copy(left.keys[left.len+1:], right.keys[:right.len])
		left.values[left.len] = n.values[i]
		copy(left.values[left.len+1:], right.values[:right.len])
		if left.branch != nil && left.branch.prefixes != nil {
			copy(left.branch.prefixes[left.len+1:],
				right.branch.prefixes[:right.len])
			left.branch.prefixes[left.len] = n.branch.prefixes[i]
		}
		if !left.leaf() {
			copy(left.branch.children[left.len+1:],
				right.branch.children[:right.len+1])
			copy(left.branch.counts[left.len+1:],
				right.branch.counts[:right.len+1])
		}
		left.len += 1 + right.len

		// move the items over one slot
		copy(n.keys[i:], n.keys[i+1:n.len])
		n.keys[n.len-1] = emptyKey
		copy(n.values[i:], n.values[i+1:n.len])
		n.values[n.len-1] = emptyValue
		if n.branch.prefixes != nil {
			copy(n.branch.prefixes[i:], n.branch.prefixes[i+1:n.len])
			n.branch.prefixes[n.len-1] = 0
		}

		// move the children over one slot
		copy(n.branch.children[i+1:], n.branch.children[i+2:n.len+1])
		n.branch.children[n.len] = nil
		copy(n.branch.counts[i+1:], n.branch.counts[i+2:n.len+1])
		n.branch.counts[n.len] = 0

		n.len--
	} else if left.len > right.len {
		// move left -> right over one slot

		// Move the item of the parent node at index into the right-node first
		// slot, and move the left-node last item into the previously moved
		// parent item slot.
		copy(right.keys[1:], right.keys[:right.len])
		right.keys[0] = n.keys[i]
		n.keys[i] = left.keys[left.len-1]
		left.keys[left.len-1] = emptyKey
		copy(right.values[1:], right.values[:right.len])
		right.values[0] = n.values[i]
		n.values[i] = left.values[left.len-1]
		left.values[left.len-1] = emptyValue
		if left.branch != nil && left.branch.prefixes != nil {
			copy(right.branch.prefixes[1:], right.branch.prefixes[:right.len])
			right.branch.prefixes[0] = n.branch.prefixes[i]
			n.branch.prefixes[i] = left.branch.prefixes[left.len-1]
			left.branch.prefixes[left.len-1] = 0
		} else if n.branch.prefixes != nil {
			n.branch.prefixes[i] = prefixString(n.keys[i])
		}
		if !left.leaf() {
			// move the left-node last child into the right-node first slot
			copy(right.branch.children[1:], right.branch.children[:right.len+1])
			right.branch.children[0] = left.branch.children[left.len]
			left.branch.children[left.len] = nil
			copy(right.branch.counts[1:], right.branch.counts[:right.len+1])
			right.branch.counts[0] = left.branch.counts[left.len]
			left.branch.counts[left.len] = 0
		}
		left.len--
		right.len++
	} else {
		// move left <- right over one slot

		// Same as above but the other direction
		left.keys[left.len] = n.keys[i]
		n.keys[i] = right.keys[0]
		copy(right.keys[:], right.keys[1:right.len])
		right.keys[right.len-1] = emptyKey
		left.values[left.len] = n.values[i]
		n.values[i] = right.values[0]
		copy(right.values[:], right.values[1:right.len])
		right.values[right.len-1] = emptyValue
		if left.branch != nil && left.branch.prefixes != nil {
			left.branch.prefixes[left.len] = n.branch.prefixes[i]
			n.branch.prefixes[i] = right.branch.prefixes[0]
			copy(right.branch.prefixes[:], right.branch.prefixes[1:right.len])
			right.branch.prefixes[right.len-1] = 0
		} else if n.branch.prefixes != nil {
			n.branch.prefixes[i] = prefixString(n.keys[i])
		}
		if !left.leaf() {
			left.branch.children[left.len+1] = right.branch.children[0]
			copy(right.branch.children[:], right.branch.children[1:right.len+1])
			right.branch.children[right.len] = nil
			left.branch.counts[left.len+1] = right.branch.counts[0]
			copy(right.branch.counts[:], right.branch.counts[1:right.len+1])
			right.branch.counts[right.len] = 0
		}
		left.len++
		right.len--
	}
	// Recalculate the counts for both right and left children.
	n.branch.counts[i] = n.branch.children[i].count()
	n.branch.counts[i+1] = n.branch.children[i+1].count()
}

func (t *tree[K, V]) nodeAll(n *node[K, V], yield func(K, V) bool, mut bool,
) bool {
	if n.leaf() {
		for i := 0; i < n.len; i++ {
			if !yield(n.keys[i], n.values[i]) {
				return false
			}
		}
		return true
	}
	for i := 0; i < n.len; i++ {
		t.cowChild(n, i, mut)
		if !t.nodeAll(n.branch.children[i], yield, mut) {
			return false
		}
		if !yield(n.keys[i], n.values[i]) {
			return false
		}
	}
	t.cowChild(n, n.len, mut)
	return t.nodeAll(n.branch.children[n.len], yield, mut)
}

func (t *tree[K, V]) nodeBackward(n *node[K, V], yield func(K, V) bool,
	mut bool,
) bool {
	if n.leaf() {
		for i := n.len - 1; i >= 0; i-- {
			if !yield(n.keys[i], n.values[i]) {
				return false
			}
		}
		return true
	}
	t.cowChild(n, n.len, mut)
	if !t.nodeBackward(n.branch.children[n.len], yield, mut) {
		return false
	}
	for i := n.len - 1; i >= 0; i-- {
		if !yield(n.keys[i], n.values[i]) {
			return false
		}
		t.cowChild(n, i, mut)
		if !t.nodeBackward(n.branch.children[i], yield, mut) {
			return false
		}
	}
	return true
}

func (t *tree[K, V]) all0(yield func(K, V) bool, mut bool) {
	if t.count > 0 {
		t.cowRoot(mut)
		t.nodeAll(t.root, yield, mut)
	}
}

func (t *tree[K, V]) backward0(yield func(K, V) bool, mut bool) {
	if t.count > 0 {
		t.cowRoot(mut)
		t.nodeBackward(t.root, yield, mut)
	}
}

func (t *tree[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		t.all0(yield, false)
	}
}

func (t *tree[K, V]) AllMut() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		t.all0(yield, true)
	}
}

func (t *tree[K, V]) Backward() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		t.backward0(yield, false)
	}
}

func (t *tree[K, V]) BackwardMut() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		t.backward0(yield, true)
	}
}

func (t *tree[K, V]) collapseRootIfNeeded() {
	if t.count == 0 {
		t.root = nil
	} else if t.root.len == 0 && !t.root.leaf() {
		t.root = t.root.branch.children[0]
	}
}

func (t *tree[K, V]) nodePopFront(n *node[K, V], c func(K, V) bool,
) (K, V, bool) {
	var emptyKey K
	var emptyValue V
	if n.leaf() {
		oldKey, oldValue := n.keys[0], n.values[0]
		if c != nil && !c(oldKey, oldValue) {
			return emptyKey, emptyValue, false
		}
		copy(n.keys[:n.len-1], n.keys[1:n.len])
		copy(n.values[:n.len-1], n.values[1:n.len])
		n.keys[n.len-1] = emptyKey
		n.values[n.len-1] = emptyValue
		n.len--
		return oldKey, oldValue, true
	}
	t.cowChild(n, 0, true)
	oldKey, oldValue, popped := t.nodePopFront(n.branch.children[0], c)
	if !popped {
		return emptyKey, emptyValue, false
	}
	n.branch.counts[0]--
	if n.branch.children[0].len < minItems {
		t.rebalance(n, 0)
	}
	return oldKey, oldValue, true
}

func (t *tree[K, V]) nodePopBack(n *node[K, V], c func(K, V) bool,
) (K, V, bool) {
	var emptyKey K
	var emptyValue V
	if n.leaf() {
		oldKey, oldValue := n.keys[n.len-1], n.values[n.len-1]
		if c != nil && !c(oldKey, oldValue) {
			return emptyKey, emptyValue, false
		}
		n.keys[n.len-1] = emptyKey
		n.values[n.len-1] = emptyValue
		n.len--
		return oldKey, oldValue, true
	}
	t.cowChild(n, n.len, true)
	oldKey, oldValue, popped := t.nodePopBack(n.branch.children[n.len], c)
	if !popped {
		return emptyKey, emptyValue, false
	}
	n.branch.counts[n.len]--
	if n.branch.children[n.len].len < minItems {
		t.rebalance(n, n.len)
	}
	return oldKey, oldValue, true
}

func (t *tree[K, V]) PopFrontFunc(cond func(key K, value V) bool) (K, V, bool) {
	var emptyKey K
	var emptyValue V
	if t.count == 0 {
		return emptyKey, emptyValue, false
	}
	t.cowRoot(true)
	oldKey, oldValue, popped := t.nodePopFront(t.root, cond)
	if !popped {
		return emptyKey, emptyValue, false
	}
	t.count--
	t.collapseRootIfNeeded()
	return oldKey, oldValue, true
}

func (t *tree[K, V]) PopFront() (K, V, bool) {
	return t.PopFrontFunc(nil)
}

func (t *tree[K, V]) PopBackFunc(cond func(key K, value V) bool) (K, V, bool) {
	var emptyKey K
	var emptyValue V
	if t.count == 0 {
		return emptyKey, emptyValue, false
	}
	t.cowRoot(true)
	oldKey, oldValue, popped := t.nodePopBack(t.root, cond)
	if !popped {
		return emptyKey, emptyValue, false
	}
	t.count--
	t.collapseRootIfNeeded()
	return oldKey, oldValue, true
}

func (t *tree[K, V]) PopBack() (K, V, bool) {
	return t.PopBackFunc(nil)
}

func (t *tree[K, V]) front0(mut bool) (K, V, bool) {
	var emptyKey K
	var emptyValue V
	if t.count == 0 {
		return emptyKey, emptyValue, false
	}
	t.cowRoot(mut)
	n := t.root
	for {
		if n.leaf() {
			return n.keys[0], n.values[0], true
		}
		t.cowChild(n, 0, mut)
		n = n.branch.children[0]
	}
}

func (t *tree[K, V]) Front() (K, V, bool) {
	return t.front0(false)
}

func (t *tree[K, V]) FrontMut() (K, V, bool) {
	return t.front0(true)
}

func (t *tree[K, V]) back0(mut bool) (K, V, bool) {
	var emptyKey K
	var emptyValue V
	if t.count == 0 {
		return emptyKey, emptyValue, false
	}
	t.cowRoot(mut)
	n := t.root
	for {
		if n.leaf() {
			return n.keys[n.len-1], n.values[n.len-1], true
		}
		t.cowChild(n, n.len, mut)
		n = n.branch.children[n.len]
	}
}

func (t *tree[K, V]) Back() (K, V, bool) {
	return t.back0(false)
}

func (t *tree[K, V]) BackMut() (K, V, bool) {
	return t.back0(true)
}

func (t *tree[K, V]) Height() int {
	if t.count == 0 {
		return 0
	}
	var height int
	n := t.root
	for !n.leaf() {
		n = n.branch.children[0]
		height++
	}
	return height
}

func (t *tree[K, V]) Len() int {
	return t.count
}

func (t *tree[K, V]) Clear() {
	t.count = 0
	t.root = nil
}

func (t *tree[K, V]) CloneInto(t2 *tree[K, V]) {
	t.cloned = true
	*t2 = *t
	if t.root != nil {
		t.root.rc.Add(1)
	}
}

func (t *tree[K, V]) Release() {
	if t.root != nil {
		t.releaseNode(t.root)
	}
	t.Clear()
}

func (t *tree[K, V]) gte(ak K, av V, bk K, bv V) bool {
	return t.dataCompare(ak, av, bk, bv) >= 0
}

func (t *tree[K, V]) lte(ak K, av V, bk K, bv V) bool {
	return t.dataCompare(ak, av, bk, bv) <= 0
}

func (t *tree[K, V]) eq(ak K, av V, bk K, bv V) bool {
	return t.dataCompare(ak, av, bk, bv) == 0
}

func (t *tree[K, V]) PushFront(key K, value V) bool {
	if t.count == 0 {
		t.insertFirstItem(key, value)
		return true
	}
	t.cowRoot(true)
	t.ensureRoot()
	n := t.root
	for {
		if n.leaf() {
			if t.dataCompare != nil && t.gte(key, value, n.keys[0],
				n.values[0]) {
				break
			}
			n.insertItemAt(key, value, 0)
			t.count++
			return true
		}
		t.cowChild(n, 0, true)
		t.ensureBranch(n, 0)
		n.branch.counts[0]++
		n = n.branch.children[0]
	}
	// out or order, rollback counts
	n = t.root
	for !n.leaf() {
		n.branch.counts[0]--
		n = n.branch.children[0]
	}
	return false
}

func (t *tree[K, V]) PushBack(key K, value V) bool {
	if t.count == 0 {
		t.insertFirstItem(key, value)
		return true
	}
	t.cowRoot(true)
	t.ensureRoot()
	n := t.root
	for {
		if n.leaf() {
			if t.dataCompare != nil && t.lte(key, value, n.keys[n.len-1],
				n.values[n.len-1]) {
				break
			}
			n.insertItemAt(key, value, n.len)
			t.count++
			return true
		}
		t.cowChild(n, n.len, true)
		t.ensureBranch(n, n.len)
		n.branch.counts[n.len]++
		n = n.branch.children[n.len]
	}
	// out or order, rollback counts
	n = t.root
	for !n.leaf() {
		n.branch.counts[n.len]--
		n = n.branch.children[n.len]
	}
	return false
}

func (t *tree[K, V]) search(n *node[K, V], key K, value V) (int, bool) {
	return t.dataSearch(n.len, &n.keys[0], &n.values[0], key, value)
}

func (t *tree[K, V]) nodeAscend(n *node[K, V], key K, value V,
	yield func(K, V) bool, mut bool,
) bool {
	i, found := t.search(n, key, value)
	if !found {
		if !n.leaf() {
			t.cowChild(n, i, mut)
			if !t.nodeAscend(n.branch.children[i], key, value, yield, mut) {
				return false
			}
		}
	}
	for ; i < n.len; i++ {
		if !yield(n.keys[i], n.values[i]) {
			return false
		}
		if !n.leaf() {
			t.cowChild(n, i+1, mut)
			if !t.nodeAll(n.branch.children[i+1], yield, mut) {
				return false
			}
		}
	}
	return true
}

func (t *tree[K, V]) ascend0(key K, value V, yield func(K, V) bool,
	mut bool,
) {
	if t.count > 0 {
		t.cowRoot(mut)
		t.nodeAscend(t.root, key, value, yield, mut)
	}
}

func (t *tree[K, V]) Ascend(key K, value V) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		t.ascend0(key, value, yield, false)
	}
}

func (t *tree[K, V]) AscendMut(key K, value V) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		t.ascend0(key, value, yield, true)
	}
}

func (t *tree[K, V]) nodeInsert(n *node[K, V], key K, value V) (V, int) {
	var emptyValue V
	i, found := t.search(n, key, value)
	if found {
		return n.values[i], 0
	}
	if n.leaf() {
		n.insertItemAt(key, value, i)
		return emptyValue, 1
	}
	t.cowChild(n, i, true)
	if t.ensureBranch(n, i) {
		return t.nodeInsert(n, key, value)
	}
	prev, inserted := t.nodeInsert(n.branch.children[i], key, value)
	n.branch.counts[i] += inserted
	return prev, inserted
}

func (t *tree[K, V]) Insert(key K, value V) (V, bool) {
	var emptyValue V
	if t.count == 0 {
		t.insertFirstItem(key, value)
		return emptyValue, true
	}
	t.cowRoot(true)
	t.ensureRoot()
	current, inserted := t.nodeInsert(t.root, key, value)
	t.count += inserted
	return current, inserted == 1
}

func (t *tree[K, V]) Replace(key K, value V) (V, bool) {
	var emptyValue V
	if t.count == 0 {
		return emptyValue, false
	}
	t.cowRoot(true)
	n := t.root
	for {
		i, found := t.search(n, key, value)
		if found {
			old := n.values[i]
			n.values[i] = value
			return old, true
		}
		if n.leaf() {
			return emptyValue, false
		}
		t.cowChild(n, i, true)
		n = n.branch.children[i]
	}
}

func (t *tree[K, V]) nodeSet(n *node[K, V], key K, value V) (V, int) {
	var emptyValue V
	i, found := t.search(n, key, value)
	if found {
		old := n.values[i]
		n.values[i] = value
		return old, 0
	}
	if n.leaf() {
		n.insertItemAt(key, value, i)
		return emptyValue, 1
	}
	t.cowChild(n, i, true)
	if t.ensureBranch(n, i) {
		return t.nodeSet(n, key, value)
	}
	prev, inserted := t.nodeSet(n.branch.children[i], key, value)
	n.branch.counts[i] += inserted
	return prev, inserted
}

func (t *tree[K, V]) Set(key K, value V) (V, bool) {
	var emptyValue V
	if t.count == 0 {
		t.insertFirstItem(key, value)
		return emptyValue, false
	}
	t.cowRoot(true)
	t.ensureRoot()
	current, inserted := t.nodeSet(t.root, key, value)
	t.count += inserted
	return current, inserted == 0
}

func (t *tree[K, V]) Contains(key K, value V) bool {
	if t.count == 0 {
		return false
	}
	n := t.root
	for {
		i, found := t.search(n, key, value)
		if found {
			return true
		}
		if n.leaf() {
			return false
		}
		n = n.branch.children[i]
	}
}

func (t *tree[K, V]) lookup0(key K, value V, mut bool) (V, bool) {
	var emptyValue V
	if t.count == 0 {
		return emptyValue, false
	}
	t.cowRoot(mut)
	n := t.root
	for {
		i, found := t.search(n, key, value)
		if found {
			return n.values[i], true
		}
		if n.leaf() {
			return emptyValue, false
		}
		t.cowChild(n, i, mut)
		n = n.branch.children[i]
	}
}

func (t *tree[K, V]) Lookup(key K, value V) (V, bool) {
	return t.lookup0(key, value, false)
}

func (t *tree[K, V]) LookupMut(key K, value V) (V, bool) {
	return t.lookup0(key, value, true)
}

func (t *tree[K, V]) Get(key K, value V) V {
	v, _ := t.lookup0(key, value, false)
	return v
}

func (t *tree[K, V]) GetMut(key K, value V) V {
	v, _ := t.lookup0(key, value, true)
	return v
}

func (t *tree[K, V]) nodeDelete(n *node[K, V], key K, value V) (V, bool) {
	var emptyKey K
	var emptyValue V
	i, found := t.search(n, key, value)
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
	t.cowChild(n, i, true)
	if found {
		old = n.values[i]
		maxKey, maxValue := t.nodeDeleteMax(n.branch.children[i])
		deleted = true
		n.keys[i] = maxKey
		n.values[i] = maxValue
	} else {
		old, deleted = t.nodeDelete(n.branch.children[i], key, value)
	}
	if !deleted {
		return old, false
	}
	n.branch.counts[i]--
	if n.branch.children[i].len < minItems {
		t.rebalance(n, i)
	}
	return old, true
}

func (t *tree[K, V]) Delete(key K, value V) (V, bool) {
	var emptyValue V
	if t.count == 0 {
		return emptyValue, false
	}
	t.cowRoot(true)
	old, deleted := t.nodeDelete(t.root, key, value)
	if deleted {
		t.count--
		t.collapseRootIfNeeded()
	}
	return old, deleted
}

func (t *tree[K, V]) nodeDescend(n *node[K, V], key K, value V,
	yield func(K, V) bool, mut bool,
) bool {
	i, found := t.search(n, key, value)
	if !found {
		if !n.leaf() {
			t.cowChild(n, i, mut)
			if !t.nodeDescend(n.branch.children[i], key, value, yield, mut) {
				return false
			}
		}
		i--
	}
	for ; i >= 0; i-- {
		if !yield(n.keys[i], n.values[i]) {
			return false
		}
		if !n.leaf() {
			t.cowChild(n, i, mut)
			if !t.nodeBackward(n.branch.children[i], yield, mut) {
				return false
			}
		}
	}
	return true
}

func (t *tree[K, V]) descend0(key K, value V, yield func(K, V) bool, mut bool) {
	if t.count > 0 {
		t.cowRoot(mut)
		t.nodeDescend(t.root, key, value, yield, mut)
	}
}

func (t *tree[K, V]) Descend(key K, value V) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		t.descend0(key, value, yield, false)
	}
}

func (t *tree[K, V]) DescendMut(key K, value V) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		t.descend0(key, value, yield, true)
	}
}

func (t *tree[K, V]) seek0(key K, value V, mut bool) (K, V, bool) {
	var rkey K
	var rvalue V
	var found bool
	if t.count > 0 {
		t.ascend0(key, value, func(ikey K, ivalue V) bool {
			rkey, rvalue, found = ikey, ivalue, true
			return false
		}, mut)
	}
	return rkey, rvalue, found
}

func (t *tree[K, V]) Seek(key K, value V) (K, V, bool) {
	return t.seek0(key, value, false)
}

func (t *tree[K, V]) SeekMut(key K, value V) (K, V, bool) {
	return t.seek0(key, value, true)
}

func (t *tree[K, V]) seekNext0(key K, value V, mut bool) (K, V, bool) {
	var rkey K
	var rvalue V
	var found bool
	t.ascend0(key, value, func(ikey K, ivalue V) bool {
		if t.eq(key, value, ikey, ivalue) {
			return true
		}
		rkey, rvalue, found = ikey, ivalue, true
		return false
	}, mut)
	return rkey, rvalue, found
}

func (t *tree[K, V]) SeekNext(key K, value V) (K, V, bool) {
	return t.seekNext0(key, value, false)
}

func (t *tree[K, V]) SeekNextMut(key K, value V) (K, V, bool) {
	return t.seekNext0(key, value, true)
}

func (t *tree[K, V]) seekPrev0(key K, value V, mut bool) (K, V, bool) {
	var rkey K
	var rvalue V
	var found bool
	t.descend0(key, value, func(ikey K, ivalue V) bool {
		if t.eq(key, value, ikey, ivalue) {
			return true
		}
		rkey, rvalue, found = ikey, ivalue, true
		return false
	}, mut)
	return rkey, rvalue, found
}

func (t *tree[K, V]) SeekPrev(key K, value V) (K, V, bool) {
	return t.seekPrev0(key, value, false)
}

func (t *tree[K, V]) SeekPrevMut(key K, value V) (K, V, bool) {
	return t.seekPrev0(key, value, true)
}

func (n *node[K, V]) getAtNoCheck(index int) (K, V) {
	for {
		if n.leaf() {
			return n.keys[index], n.values[index]
		}
		i := 0
		for ; i < n.len; i++ {
			count := n.branch.counts[i]
			if index < count {
				break
			}
			if index == count {
				return n.keys[i], n.values[i]
			}
			index -= count + 1
		}
		n = n.branch.children[i]
	}
}

func (t *tree[K, V]) InsertAt(index int, key K, value V) bool {
	if index < 0 || index > t.count {
		return false
	}
	if index == 0 {
		return t.PushFront(key, value)
	}
	if index == t.count {
		return t.PushBack(key, value)
	}
	if t.dataCompare != nil {
		if index > 0 {
			mkey, mval := t.root.getAtNoCheck(index - 1)
			if t.gte(mkey, mval, key, value) {
				return false
			}
		}
		if index < t.count {
			mkey, mval := t.root.getAtNoCheck(index)
			if t.gte(key, value, mkey, mval) {
				return false
			}
		}
	}
	t.cowRoot(true)
	t.ensureRoot()
	n := t.root
	for {
		index0 := index
		if n.leaf() {
			n.insertItemAt(key, value, index)
			t.count++
			return true
		}
		i := 0
		for ; i < n.len; i++ {
			count := n.branch.counts[i]
			if index <= count {
				break
			}
			index -= count + 1
		}
		t.cowChild(n, i, true)
		if t.ensureBranch(n, i) {
			index = index0
			continue
		}
		n.branch.counts[i]++
		n = n.branch.children[i]
	}
}

func (t *tree[K, V]) ReplaceAt(index int, key K, value V) (K, V, bool) {
	var emptyKey K
	var emptyValue V
	if index < 0 || index >= t.count {
		return emptyKey, emptyValue, false
	}
	if t.dataCompare != nil {
		if index > 0 {
			mkey, mval := t.root.getAtNoCheck(index - 1)
			if t.gte(mkey, mval, key, value) {
				return emptyKey, emptyValue, false
			}
		}
		if index < t.count-1 {
			mkey, mval := t.root.getAtNoCheck(index + 1)
			if t.gte(key, value, mkey, mval) {
				return emptyKey, emptyValue, false
			}
		}
	}
	t.cowRoot(true)
	n := t.root
	for {
		if n.leaf() {
			i := index
			oldKey, oldValue := n.keys[i], n.values[i]
			n.keys[i], n.values[i] = key, value
			return oldKey, oldValue, true
		}
		i := 0
		for ; i < n.len; i++ {
			count := n.branch.counts[i]
			if index < count {
				break
			}
			if index == count {
				oldKey, oldValue := n.keys[i], n.values[i]
				n.keys[i], n.values[i] = key, value
				return oldKey, oldValue, true
			}
			index -= count + 1
		}
		t.cowChild(n, i, true)
		n = n.branch.children[i]
	}
}

func (t *tree[K, V]) nodeDeleteAt(n *node[K, V], index int) (K, V) {
	var emptyKey K
	var emptyValue V
	if n.leaf() {
		i := index
		oldKey, oldValue := n.keys[i], n.values[i]
		copy(n.keys[i:n.len-1], n.keys[i+1:n.len])
		n.keys[n.len-1] = emptyKey
		copy(n.values[i:n.len-1], n.values[i+1:n.len])
		n.values[n.len-1] = emptyValue
		n.len--
		return oldKey, oldValue
	}
	var i int
	var found bool
	for ; i < n.len; i++ {
		count := n.branch.counts[i]
		if index <= count {
			found = index == count
			break
		}
		index -= count + 1
	}
	var oldKey K
	var oldValue V
	t.cowChild(n, i, true)
	if found {
		oldKey, oldValue = n.keys[i], n.values[i]
		n.keys[i], n.values[i] = t.nodeDeleteMax(n.branch.children[i])
	} else {
		oldKey, oldValue = t.nodeDeleteAt(n.branch.children[i], index)
	}
	n.branch.counts[i]--
	if n.branch.children[i].len < minItems {
		t.rebalance(n, i)
	}
	return oldKey, oldValue
}

func (t *tree[K, V]) DeleteAt(index int) (K, V, bool) {
	var emptyKey K
	var emptyValue V
	if index < 0 || index >= t.count {
		return emptyKey, emptyValue, false
	}
	t.cowRoot(true)
	oldKey, oldValue := t.nodeDeleteAt(t.root, index)
	t.count--
	t.collapseRootIfNeeded()
	return oldKey, oldValue, true
}

func (t *tree[K, V]) lookupAt0(index int, mut bool) (K, V, bool) {
	var emptyKey K
	var emptyValue V
	if index < 0 || index >= t.count {
		return emptyKey, emptyValue, false
	}
	if index == 0 {
		return t.front0(mut)
	}
	if index == t.count-1 {
		return t.back0(mut)
	}
	t.cowRoot(mut)
	n := t.root
	for {
		if n.leaf() {
			return n.keys[index], n.values[index], true
		}
		i := 0
		for ; i < n.len; i++ {
			count := n.branch.counts[i]
			if index < count {
				break
			}
			if index == count {
				return n.keys[i], n.values[i], true
			}
			index -= count + 1
		}
		t.cowChild(n, i, mut)
		n = n.branch.children[i]
	}
}

func (t *tree[K, V]) LookupAt(index int) (K, V, bool) {
	return t.lookupAt0(index, false)
}

func (t *tree[K, V]) LookupAtMut(index int) (K, V, bool) {
	return t.lookupAt0(index, true)
}

func (t *tree[K, V]) GetAt(index int) (K, V) {
	k, v, _ := t.lookupAt0(index, false)
	return k, v
}

func (t *tree[K, V]) GetAtMut(index int) (K, V) {
	k, v, _ := t.lookupAt0(index, true)
	return k, v
}

func (t *tree[K, V]) IndexOf(key K, value V) (int, bool) {
	if t.count == 0 {
		return 0, false
	}
	n := t.root
	var index, depth int
	for {
		i, found := t.search(n, key, value)
		index += i
		if n.leaf() {
			if found {
				return index, true
			}
			return index, false
		}
		for j := range i {
			index += n.branch.counts[j]
		}
		if found {
			index += n.branch.counts[i]
			return index, true
		}
		n = n.branch.children[i]
		depth++
	}
}

func (t *tree[K, V]) nodeAscendAt(n *node[K, V], index int,
	yield func(K, V) bool, mut bool,
) bool {
	if n.leaf() {
		for i := index; i < n.len; i++ {
			if !yield(n.keys[i], n.values[i]) {
				return false
			}
		}
		return true
	}
	keepSearching := true
	i := 0
	for ; i < n.len; i++ {
		count := n.branch.counts[i]
		if index < count {
			break
		}
		if index == count {
			keepSearching = false
			break
		}
		index -= count + 1
	}
	if keepSearching {
		t.cowChild(n, i, mut)
		if !t.nodeAscendAt(n.branch.children[i], index, yield, mut) {
			return false
		}
	}
	for ; i < n.len; i++ {
		if !yield(n.keys[i], n.values[i]) {
			return false
		}
		t.cowChild(n, i+1, mut)
		if !t.nodeAll(n.branch.children[i+1], yield, mut) {
			return false
		}
	}
	return true
}

func (t *tree[K, V]) ascendAt0(index int, yield func(K, V) bool, mut bool) {
	if index <= 0 || index >= t.count {
		if index <= 0 {
			t.all0(yield, mut)
		}
	} else if t.count > 0 {
		t.cowRoot(mut)
		t.nodeAscendAt(t.root, index, yield, mut)
	}
}

func (t *tree[K, V]) nodeDescendAt(n *node[K, V], index int,
	yield func(K, V) bool, mut bool,
) bool {
	if n.leaf() {
		for i := n.len - index - 1; i >= 0; i-- {
			if !yield(n.keys[i], n.values[i]) {
				return false
			}
		}
		return true
	}
	keepSearching := true
	i := n.len
	for ; i > 0; i-- {
		count := n.branch.counts[i]
		if index < count {
			break
		}
		if index == count {
			keepSearching = false
			break
		}
		index -= count + 1
	}
	if keepSearching {
		t.cowChild(n, i, mut)
		if !t.nodeDescendAt(n.branch.children[i], index, yield, mut) {
			return false
		}
	}
	i--
	for ; i >= 0; i-- {
		if !yield(n.keys[i], n.values[i]) {
			return false
		}
		t.cowChild(n, i, mut)
		if !t.nodeBackward(n.branch.children[i], yield, mut) {
			return false
		}
	}
	return true
}

func (t *tree[K, V]) descendAt0(index int, yield func(K, V) bool, mut bool) {
	if index < 0 || index >= t.count-1 {
		if index >= t.count-1 {
			t.backward0(yield, mut)
		}
	} else if t.count > 0 {
		index = t.count - index - 1
		t.cowRoot(mut)
		t.nodeDescendAt(t.root, index, yield, mut)
	}
}

func (t *tree[K, V]) AscendAt(index int) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		t.ascendAt0(index, yield, false)
	}
}

func (t *tree[K, V]) AscendAtMut(index int) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		t.ascendAt0(index, yield, true)
	}
}

func (t *tree[K, V]) DescendAt(index int) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		t.descendAt0(index, yield, false)
	}
}

func (t *tree[K, V]) DescendAtMut(index int) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		t.descendAt0(index, yield, true)
	}
}

// Extract2 is a slice of items that can be iterated forward or backward.
type Extract2[K, V any] interface {
	Len() int
	All() iter.Seq2[K, V]
	Backward() iter.Seq2[K, V]
	Keys() iter.Seq[K]
	Values() iter.Seq[V]
}

// Extract is a slice of items that can be iterated forward or backward.
type Extract[V any] interface {
	Len() int
	All() iter.Seq[V]
	Backward() iter.Seq[V]
}

// extract2 is a binary tree of nodes and/or items
type extract2[K, V any] struct {
	count   int
	entries []extractEntry[K, V]
	left    *extract2[K, V]
	right   *extract2[K, V]
}

type extract2Item[K, V any] struct {
	key   K
	value V
}

type extractEntry[K, V any] struct {
	node  *node[K, V]          // node first ...
	items []extract2Item[K, V] // .. then items
}

func (s *extract2[K, V]) all0(yield func(K, V) bool) bool {
	if s.left != nil && !s.left.all0(yield) {
		return false
	}
	for _, e := range s.entries {
		if e.node != nil {
			if !(*tree[K, V])(nil).nodeAll(e.node, yield, false) {
				return false
			}
		}
		for i := range e.items {
			if !yield(e.items[i].key, e.items[i].value) {
				return false
			}
		}
	}
	if s.right != nil && !s.right.all0(yield) {
		return false
	}
	return true
}

func (s *extract2[K, V]) backward0(yield func(K, V) bool) bool {
	if s.right != nil && !s.right.backward0(yield) {
		return false
	}
	for i := len(s.entries) - 1; i >= 0; i-- {
		e := s.entries[i]
		for j := len(e.items) - 1; j >= 0; j-- {
			if !yield(e.items[j].key, e.items[j].value) {
				return false
			}
		}
		if e.node != nil {
			if !((*tree[K, V])(nil)).nodeBackward(e.node, yield, false) {
				return false
			}
		}
	}
	if s.left != nil && !s.left.backward0(yield) {
		return false
	}
	return true
}

func (s *extract2[K, V]) pushItem(key K, value V) {
	if len(s.entries) == 0 {
		s.entries = append(s.entries, extractEntry[K, V]{})
	}
	s.entries[len(s.entries)-1].items =
		append(s.entries[len(s.entries)-1].items, extract2Item[K, V]{key, value})
	s.count++
}
func (s *extract2[K, V]) pushNode(n *node[K, V], count int) {
	s.entries = append(s.entries, extractEntry[K, V]{node: n})
	s.count += count
}

func (s *extract2[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		if s != nil {
			s.all0(yield)
		}
	}
}

func (s *extract2[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		if s != nil {
			s.all0(func(key K, _ V) bool {
				return yield(key)
			})
		}
	}
}

func (s *extract2[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		if s != nil {
			s.all0(func(_ K, value V) bool {
				return yield(value)
			})
		}
	}
}

func (s *extract2[K, V]) Backward() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		if s != nil {
			s.backward0(yield)
		}
	}
}

func (s *extract2[K, V]) Len() int {
	if s == nil {
		return 0
	}
	return s.count
}

type delrange struct {
	start int
	count int
}

func (t *tree[K, V]) nodeDeleteRangeAt(n *node[K, V], offset, index, count int,
	list *extract2[K, V], depth int, noret bool,
) (dr delrange) {
	var emptyKey K
	var emptyValue V
	if n.leaf() {
		if index+count > n.len {
			count = n.len - index
		}
		i := index
		limit := minItems - 1
		if depth == 0 {
			limit = -1
		}
		dr.start = offset + i
		nlen := n.len
		for j := i; count > 0 && j < n.len && nlen > limit; j++ {
			if !noret {
				list.pushItem(n.keys[j], n.values[j])
			}
			nlen--
			count--
			dr.count++
		}
		copy(n.keys[i:], n.keys[i+dr.count:n.len])
		copy(n.values[i:], n.values[i+dr.count:n.len])
		n.len -= dr.count
		for j := 0; j < dr.count; j++ {
			n.keys[n.len+j] = emptyKey
			n.values[n.len+j] = emptyValue
		}
		return dr
	}
	var i int
	var found bool
	for ; i < n.len; i++ {
		count := n.branch.counts[i]
		if index <= count {
			if index == count {
				offset += count
				found = true
			}
			break
		}
		index -= 1 + count
		offset += 1 + count
	}
	if found && n.len > minItems && count > n.branch.counts[i+1] {
		// take entire node
		dr.start = offset
		dr.count = n.branch.counts[i+1] + 1
		if !noret {
			list.pushItem(n.keys[i], n.values[i])
			list.pushNode(n.branch.children[i+1], n.branch.counts[i+1])
		}
		// count -= n.branch.counts[i+1] + 1
		copy(n.keys[i:], n.keys[i+1:n.len])
		n.keys[n.len-1] = emptyKey
		copy(n.values[i:], n.values[i+1:n.len])
		n.values[n.len-1] = emptyValue
		if n.branch.prefixes != nil {
			copy(n.branch.prefixes[i:], n.branch.prefixes[i+1:n.len])
			n.branch.prefixes[n.len-1] = 0
		}
		copy(n.branch.counts[i+1:], n.branch.counts[i+2:n.len+1])
		n.branch.counts[n.len] = 0
		copy(n.branch.children[i+1:], n.branch.children[i+2:n.len+1])
		n.branch.children[n.len] = nil
		n.len--
		return dr
	}

	if found {
		// take one item
		dr.start = offset
		dr.count = 1
		if !noret {
			list.pushItem(n.keys[i], n.values[i])
		}
		t.cowChild(n, i, true)
		n.keys[i], n.values[i] = t.nodeDeleteMax(n.branch.children[i])
		if n.branch.prefixes != nil {
			n.branch.prefixes[i] = prefixString(n.keys[i])
		}
		// count--
		n.branch.counts[i]--
		if n.branch.children[i].len < minItems {
			t.rebalance(n, i)
		}
		return dr
	}

	var taken int
	for index == 0 && i < n.len && n.len > minItems &&
		count > n.branch.counts[i] {
		// take CURRENT entire node and CURRENT item
		if taken == 0 {
			dr.start = offset
		}
		dr.count += n.branch.counts[i] + 1
		if !noret {
			list.pushNode(n.branch.children[i], n.branch.counts[i])
			list.pushItem(n.keys[i], n.values[i])
		}
		count -= n.branch.counts[i] + 1
		copy(n.keys[i:], n.keys[i+1:n.len])
		n.keys[n.len-1] = emptyKey
		copy(n.values[i:], n.values[i+1:n.len])
		n.values[n.len-1] = emptyValue
		if n.branch.prefixes != nil {
			copy(n.branch.prefixes[i:], n.branch.prefixes[i+1:n.len])
			n.branch.prefixes[n.len-1] = 0
		}
		copy(n.branch.counts[i:], n.branch.counts[i+1:n.len+1])
		n.branch.counts[n.len] = 0
		copy(n.branch.children[i:], n.branch.children[i+1:n.len+1])
		n.branch.children[n.len] = nil
		n.len--
		taken++
	}
	if taken > 0 {
		return dr
	}
	for index > 0 && i < n.len && n.len > minItems &&
		count > (n.branch.counts[i]-index)+n.branch.counts[i+1]+1 {
		// take CURRENT item and NEXT entire node
		if taken == 0 {
			dr.start = offset + n.branch.counts[i]
		}
		dr.count += n.branch.counts[i+1] + 1
		if !noret {
			list.pushItem(n.keys[i], n.values[i])
			list.pushNode(n.branch.children[i+1], n.branch.counts[i+1])
		}
		count -= n.branch.counts[i+1] + 1
		copy(n.keys[i:], n.keys[i+1:n.len])
		n.keys[n.len-1] = emptyKey
		copy(n.values[i:], n.values[i+1:n.len])
		n.values[n.len-1] = emptyValue
		if n.branch.prefixes != nil {
			copy(n.branch.prefixes[i:], n.branch.prefixes[i+1:n.len])
			n.branch.prefixes[n.len-1] = 0
		}
		copy(n.branch.counts[i+1:], n.branch.counts[i+2:n.len+1])
		n.branch.counts[n.len] = 0
		copy(n.branch.children[i+1:], n.branch.children[i+2:n.len+1])
		n.branch.children[n.len] = nil
		n.len--
		taken++
	}
	if taken > 0 {
		return dr
	}
	// Recursively search for items or nodes to delete
	t.cowChild(n, i, true)
	dr = t.nodeDeleteRangeAt(n.branch.children[i], offset, index, count, list,
		depth+1, noret)
	n.branch.counts[i] -= dr.count
	if n.branch.children[i].len < minItems {
		t.rebalance(n, i)
	}
	return dr
}

func (t *tree[K, V]) deleteRangeAt1(index, count int, noret bool,
) (list extract2[K, V], dr delrange) {
	if t.count > 0 {
		t.cowRoot(true)
		dr = t.nodeDeleteRangeAt(t.root, 0, index, count, &list, 0, noret)
		t.count -= dr.count
		t.collapseRootIfNeeded()
	}
	return list, dr
}

func (t *tree[K, V]) deleteRangeAt0(index, count int, noret bool) extract2[K, V] {
	end := index + count
	list, dr := t.deleteRangeAt1(index, count, noret)
	drend := dr.start + dr.count
	if dr.start > index {
		left := t.deleteRangeAt0(index, dr.start-index, noret)
		if !noret {
			list.left = &left
			list.count += left.count
		}
	}
	if drend < end {
		right := t.deleteRangeAt0(index, end-drend, noret)
		if !noret {
			list.right = &right
			list.count += right.count
		}
	}
	return list
}

// DrainOptions for deleting a range of items from a collection.
type DrainOptions struct {
	NoReturn     bool // Do not return items
	MinExclusive bool // Exclude the min arg
	MaxInclusive bool // Include the max arg
}

func (t *tree[K, V]) DrainAt(index, count int, opts DrainOptions,
) Extract2[K, V] {
	noret := opts.NoReturn
	if opts.MinExclusive {
		index++
		count--
	}
	if opts.MaxInclusive {
		count++
	}
	if index < 0 {
		index = 0
	}
	if count < 0 {
		count = 0
	}
	if index+count > t.count {
		count = t.count - index
	}
	if count == 0 {
		return (*extract2[K, V])(nil)
	}
	if index == 0 && count == t.count {
		var list extract2[K, V]
		if !noret {
			list.pushNode(t.root, t.count)
		}
		t.Clear()
		return &list
	}
	list := t.deleteRangeAt0(index, count, noret)
	if noret {
		return (*extract2[K, V])(nil)
	}
	return &list
}

func (t *tree[K, V]) Drain(minKey K, minValue V, maxKey K, maxValue V,
	opts DrainOptions,
) Extract2[K, V] {
	start, ok := t.IndexOf(minKey, minValue)
	if ok && opts.MinExclusive {
		start++
	}
	end, ok := t.IndexOf(maxKey, maxValue)
	if ok && opts.MaxInclusive {
		end++
	}
	opts.MinExclusive = false
	opts.MaxInclusive = false
	return t.DrainAt(start, end-start, opts)
}

type slice[T any] struct{ base any }

func (s *slice[T]) All() iter.Seq[T] {
	return func(yield func(T) bool) {
		if s != nil {
			switch s := s.base.(type) {
			case *extract2[T, omit]:
				for k := range s.All() {
					if !yield(k) {
						break
					}
				}
			case *extract2[omit, T]:
				for _, v := range s.All() {
					if !yield(v) {
						break
					}
				}
			}
		}
	}
}

func (s *slice[T]) Backward() iter.Seq[T] {
	return func(yield func(T) bool) {
		if s != nil {
			switch s := s.base.(type) {
			case *extract2[T, omit]:
				for k := range s.Backward() {
					if !yield(k) {
						break
					}
				}
			case *extract2[omit, T]:
				for _, v := range s.Backward() {
					if !yield(v) {
						break
					}
				}
			}
		}
	}
}

func (s *slice[T]) Len() int {
	if s != nil {
		switch s := s.base.(type) {
		case *extract2[T, omit]:
			return s.Len()
		case *extract2[omit, T]:
			return s.Len()
		}
	}
	return 0
}

func sliceK[T any](s2 Extract2[T, omit]) *slice[T] {
	var s1 *slice[T]
	if s2 != nil {
		s1 = &slice[T]{s2}
	}
	return s1
}

func sliceV[T any](s2 Extract2[omit, T]) *slice[T] {
	var s1 *slice[T]
	if s2 != nil {
		s1 = &slice[T]{s2}
	}
	return s1
}

func iterK[K, V any](iyield iter.Seq2[K, V]) iter.Seq[K] {
	return func(yield func(K) bool) {
		for k := range iyield {
			if !yield(k) {
				break
			}
		}
	}
}

func iterV[K, V any](iyield iter.Seq2[K, V]) iter.Seq[V] {
	return func(yield func(V) bool) {
		for _, v := range iyield {
			if !yield(v) {
				break
			}
		}
	}
}

func prefixString[K any](key K) uint64 {
	s := *(*string)(unsafe.Pointer(&key))
	var b [8]byte
	copy(b[:], s[:])
	return uint64(b[7])<<0 | uint64(b[6])<<8 | uint64(b[5])<<16 |
		uint64(b[4])<<24 | uint64(b[3])<<32 | uint64(b[2])<<40 |
		uint64(b[1])<<48 | uint64(b[0])<<56
}
