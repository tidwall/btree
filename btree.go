// https://github.com/tidwall/btree
//
// Copyright 2026 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
//
// btree - B-tree based collections for go
package btree

import (
	"cmp"
	"iter"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
	"unicode/utf8"
	"unsafe"
)

const fanout = 64
const maxItems = fanout - 1
const minItems = maxItems / 2

type tree[K, V any] struct {
	copied      bool        // Copy called at least once during life of tree.
	initd       bool        // Flag used by outer types
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
}

type branchAlloc[K, V any] struct {
	node   node[K, V]
	branch branch[K, V]
}

type omit struct{}

func (t *tree[K, V]) newNode(leaf bool) *node[K, V] {
	var n *node[K, V]
	if leaf {
		n = new(node[K, V])
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
	if mut && t.copied && t.root != nil && t.root.rc.Load() > 0 {
		t.cow0(&t.root)
	}
}

// should inline
func (t *tree[K, V]) cowChild(n *node[K, V], i int, mut bool) {
	if mut && t.copied && n.branch.children[i].rc.Load() > 0 {
		t.cow0(&n.branch.children[i])
	}
}

func (t *tree[K, V]) nodeDeleteMax(n *node[K, V]) (K, V) {
	var emptyKey K
	var emptyValue V
	var i int
	i = n.len - 1
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

func (t *tree[K, V]) Drain() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for {
			k, v, ok := t.PopFront()
			if !ok || !yield(k, v) {
				break
			}
		}
	}
}

func (t *tree[K, V]) DrainBackward() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for {
			k, v, ok := t.PopBack()
			if !ok || !yield(k, v) {
				break
			}
		}
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

func (t *tree[K, V]) PopFrontIf(cond func(key K, value V) bool) (K, V, bool) {
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
	return t.PopFrontIf(nil)
}

func (t *tree[K, V]) PopBackIf(cond func(key K, value V) bool) (K, V, bool) {
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
	return t.PopBackIf(nil)
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

func (t *tree[K, V]) CopyInto(t2 *tree[K, V]) {
	t.copied = true
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

func (t *tree[K, V]) get0(key K, value V, mut bool) (V, bool) {
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

func (t *tree[K, V]) Get(key K, value V) (V, bool) {
	return t.get0(key, value, false)
}

func (t *tree[K, V]) GetMut(key K, value V) (V, bool) {
	return t.get0(key, value, true)
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

func (t *tree[K, V]) getAt0(index int, mut bool) (K, V, bool) {
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

func (t *tree[K, V]) GetAt(index int) (K, V, bool) {
	return t.getAt0(index, false)
}

func (t *tree[K, V]) GetAtMut(index int) (K, V, bool) {
	return t.getAt0(index, true)
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

type Slice2[K, V any] interface {
	Len() int
	All() iter.Seq2[K, V]
	Backward() iter.Seq2[K, V]
	Keys() iter.Seq[K]
	Values() iter.Seq[V]
}

type Slice[V any] interface {
	Len() int
	All() iter.Seq[V]
	Backward() iter.Seq[V]
}

// slice2 is a binary tree of nodes and/or items
type slice2[K, V any] struct {
	count   int
	entries []sliceEntry[K, V]
	left    *slice2[K, V]
	right   *slice2[K, V]
}

type slice2Item[K, V any] struct {
	key   K
	value V
}

type sliceEntry[K, V any] struct {
	node  *node[K, V]        // node first ...
	items []slice2Item[K, V] // .. then items
}

func (s *slice2[K, V]) all0(yield func(K, V) bool) bool {
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

func (s *slice2[K, V]) backward0(yield func(K, V) bool) bool {
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

func (s *slice2[K, V]) pushItem(key K, value V) {
	if len(s.entries) == 0 {
		s.entries = append(s.entries, sliceEntry[K, V]{})
	}
	s.entries[len(s.entries)-1].items =
		append(s.entries[len(s.entries)-1].items, slice2Item[K, V]{key, value})
	s.count++
}
func (s *slice2[K, V]) pushNode(n *node[K, V], count int) {
	s.entries = append(s.entries, sliceEntry[K, V]{node: n})
	s.count += count
}

func (s *slice2[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		if s != nil {
			s.all0(yield)
		}
	}
}

func (s *slice2[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		if s != nil {
			s.all0(func(key K, _ V) bool {
				return yield(key)
			})
		}
	}
}

func (s *slice2[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		if s != nil {
			s.all0(func(_ K, value V) bool {
				return yield(value)
			})
		}
	}
}

func (s *slice2[K, V]) Backward() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		if s != nil {
			s.backward0(yield)
		}
	}
}

func (s *slice2[K, V]) Len() int {
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
	list *slice2[K, V], depth int, noret bool,
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
		count -= n.branch.counts[i+1] + 1
		copy(n.keys[i:], n.keys[i+1:n.len])
		n.keys[n.len-1] = emptyKey
		copy(n.values[i:], n.values[i+1:n.len])
		n.values[n.len-1] = emptyValue
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
		count--
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
) (list slice2[K, V], dr delrange) {
	if t.count > 0 {
		t.cowRoot(true)
		dr = t.nodeDeleteRangeAt(t.root, 0, index, count, &list, 0, noret)
		t.count -= dr.count
		t.collapseRootIfNeeded()
	}
	return list, dr
}

func (t *tree[K, V]) deleteRangeAt0(index, count int, noret bool) slice2[K, V] {
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

type DeleteRangeOptions struct {
	NoReturn     bool
	MinExclusive bool
	MaxInclusive bool
}

func (t *tree[K, V]) DeleteRangeAt(index, count int, opts DeleteRangeOptions,
) Slice2[K, V] {
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
		return (*slice2[K, V])(nil)
	}
	if index == 0 && count == t.count {
		var list slice2[K, V]
		if !noret {
			list.pushNode(t.root, t.count)
		}
		t.Clear()
		return &list
	}
	list := t.deleteRangeAt0(index, count, noret)
	if noret {
		return (*slice2[K, V])(nil)
	}
	return &list
}

func (t *tree[K, V]) DeleteRange(minKey K, minValue V, maxKey K, maxValue V,
	opts DeleteRangeOptions,
) Slice2[K, V] {
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
	return t.DeleteRangeAt(start, end-start, opts)
}

type slice[T any] struct{ base any }

func (s *slice[T]) All() iter.Seq[T] {
	return func(yield func(T) bool) {
		if s != nil {
			switch s := s.base.(type) {
			case *slice2[T, omit]:
				for k := range s.All() {
					if !yield(k) {
						break
					}
				}
			case *slice2[omit, T]:
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
			case *slice2[T, omit]:
				for k := range s.Backward() {
					if !yield(k) {
						break
					}
				}
			case *slice2[omit, T]:
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
		case *slice2[T, omit]:
			return s.Len()
		case *slice2[omit, T]:
			return s.Len()
		}
	}
	return 0
}

func sliceK[T any](s2 Slice2[T, omit]) *slice[T] {
	var s1 *slice[T]
	if s2 != nil {
		s1 = &slice[T]{s2}
	}
	return s1
}

func sliceV[T any](s2 Slice2[omit, T]) *slice[T] {
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

////////////////////////////////////////////////////////////////////////////////
// Array
////////////////////////////////////////////////////////////////////////////////

type ArrayOptions[T any] struct {
	Copy    func(T) T
	Release func(T)
}

type Array[T any] struct {
	tree tree[omit, T]
}

func NewArrayOptions[T any](opts ArrayOptions[T]) *Array[T] {
	b := new(Array[T])
	b.tree.dataCopy = opts.Copy
	b.tree.dataRelease = opts.Release
	return b
}

func NewList[T any]() *Array[T] {
	return NewArrayOptions(ArrayOptions[T]{})
}

func (b *Array[T]) Insert(index int, item T) bool {
	return b.tree.InsertAt(index, omit{}, item)
}

func (b *Array[T]) Replace(index int, item T) (T, bool) {
	_, value, ok := b.tree.ReplaceAt(index, omit{}, item)
	return value, ok
}

func (b *Array[T]) Delete(index int) (T, bool) {
	_, value, ok := b.tree.DeleteAt(index)
	return value, ok
}

func (b *Array[T]) Get(index int) (T, bool) {
	_, value, ok := b.tree.GetAt(index)
	return value, ok
}

func (b *Array[T]) DeleteRangeOptions(index, count int,
	opts DeleteRangeOptions,
) Slice[T] {
	return sliceV(b.tree.DeleteRangeAt(index, count, opts))
}

func (b *Array[T]) DeleteRange(index, count int) Slice[T] {
	return b.DeleteRangeOptions(index, count, DeleteRangeOptions{})
}

func (b *Array[T]) Len() int {
	return b.tree.Len()
}

func (b *Array[T]) Clear() {
	b.tree.Clear()
}

func (b *Array[T]) Release() {
	b.tree.Release()
}

func (b *Array[T]) Copy() *Array[T] {
	t2 := new(Array[T])
	*t2 = *b
	b.tree.CopyInto(&t2.tree)
	return t2
}

func (b *Array[T]) All() iter.Seq[T] {
	return iterV(b.tree.All())
}

func (b *Array[T]) Backward() iter.Seq[T] {
	return iterV(b.tree.Backward())
}

func (b *Array[T]) Ascend(index int) iter.Seq[T] {
	return iterV(b.tree.AscendAt(index))
}

func (b *Array[T]) Descend(index int) iter.Seq[T] {
	return iterV(b.tree.DescendAt(index))
}

func (b *Array[T]) PushFront(item T) bool {
	return b.tree.PushFront(omit{}, item)
}

func (b *Array[T]) PushBack(item T) bool {
	return b.tree.PushBack(omit{}, item)
}

func (b *Array[T]) Front() (T, bool) {
	_, value, ok := b.tree.Front()
	return value, ok
}

func (b *Array[T]) Back() (T, bool) {
	_, value, ok := b.tree.Back()
	return value, ok
}

func (b *Array[T]) PopFront() (T, bool) {
	_, value, ok := b.tree.PopFront()
	return value, ok
}

func (b *Array[T]) PopBack() (T, bool) {
	_, value, ok := b.tree.PopBack()
	return value, ok
}

func (b *Array[T]) PopFrontIf(cond func(item T) bool) (T, bool) {
	if cond == nil {
		return b.PopFront()
	}
	_, value, ok := b.tree.PopFrontIf(func(_ omit, item T) bool {
		return cond(item)
	})
	return value, ok
}

func (b *Array[T]) PopBackIf(cond func(item T) bool) (T, bool) {
	if cond == nil {
		return b.PopBack()
	}
	_, value, ok := b.tree.PopBackIf(func(_ omit, item T) bool {
		return cond(item)
	})
	return value, ok
}

func (b *Array[T]) Append(items ...T) {
	for _, item := range items {
		b.PushBack(item)
	}
}

func (b *Array[T]) Drain() iter.Seq[T] {
	return iterV(b.tree.Drain())
}

func (b *Array[T]) DrainBackward() iter.Seq[T] {
	return iterV(b.tree.DrainBackward())
}

////////////////////////////////////////////////////////////////////////////////
// Map
////////////////////////////////////////////////////////////////////////////////

type Map[K cmp.Ordered, V any] struct {
	tree tree[K, V]
}

type MapOptions[K cmp.Ordered, V any] struct {
	Copy    func(V) V
	Release func(V)
}

func NewMapOptions[K cmp.Ordered, V any](opts MapOptions[K, V]) *Map[K, V] {
	b := new(Map[K, V])
	b.tree.dataCopy = opts.Copy
	b.tree.dataRelease = opts.Release
	b.init()
	return b
}

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
	keys := unsafe.Slice(p, len)
	if key > keys[len-1] {
		return len, false
	}
	low, high := 0, len
	for low < high {
		h := (low + high) >> 1
		if !(key < keys[h]) {
			low = h + 1
		} else {
			high = h
		}
	}
	if low > 0 && !(keys[low-1] < key) {
		return low - 1, true
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
	if b.tree.copied {
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
	if b.tree.copied {
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
	if b.tree.copied {
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
	if b.tree.copied {
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

func mapGet[K cmp.Ordered, V any](b *Map[K, V], key K) (V, bool) {
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

func (b *Map[K, V]) Insert(key K, value V) (V, bool) {
	return mapInsert(b, key, value)
}

func (b *Map[K, V]) Replace(key K, value V) (V, bool) {
	return mapReplace(b, key, value)
}

func (b *Map[K, V]) Set(key K, value V) (V, bool) {
	return mapSet(b, key, value)
}

func (b *Map[K, V]) Delete(key K) (V, bool) {
	return mapDelete(b, key)
}

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

func (b *Map[K, V]) Get(key K) (V, bool) {
	return mapGet(b, key)
}

func (b *Map[K, V]) GetMut(key K) (V, bool) {
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

type Action int

const (
	ActionGet Action = iota
	ActionDelete
	ActionReplace
)

func (b *Map[K, V]) GetAction(key K, value func(V) (V, Action)) {
	var emptyKey K
	var emptyValue V
	if b.tree.count == 0 || value == nil {
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
				val, act := value(n.values[i])
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

func (b *Map[K, V]) Ascend(key K) iter.Seq2[K, V] {
	var emptyValue V
	return b.tree.Ascend(key, emptyValue)
}

func (b *Map[K, V]) AscendMut(key K) iter.Seq2[K, V] {
	var emptyValue V
	return b.tree.AscendMut(key, emptyValue)
}

func (b *Map[K, V]) Len() int {
	return b.tree.Len()
}

func (b *Map[K, V]) Clear() {
	b.tree.Clear()
}

func (b *Map[K, V]) Release() {
	b.tree.Release()
}

func (b *Map[K, V]) copyInto(b2 *Map[K, V]) {
	*b2 = *b
	b.tree.CopyInto(&b2.tree)
}

func (b *Map[K, V]) Copy() *Map[K, V] {
	b2 := new(Map[K, V])
	b.copyInto(b2)
	return b2
}

func (b *Map[K, V]) DeleteRangeAtOptions(index, count int,
	opts DeleteRangeOptions,
) Slice2[K, V] {
	return b.tree.DeleteRangeAt(index, count, opts)
}

func (b *Map[K, V]) DeleteRangeOptions(min, max K, opts DeleteRangeOptions,
) Slice2[K, V] {
	var emptyValue V
	return b.tree.DeleteRange(min, emptyValue, max, emptyValue, opts)
}

func (b *Map[K, V]) DeleteRangeAt(index, count int) Slice2[K, V] {
	return b.DeleteRangeAtOptions(index, count, DeleteRangeOptions{})
}

func (b *Map[K, V]) DeleteRange(min, max K) Slice2[K, V] {
	return b.DeleteRangeOptions(min, max, DeleteRangeOptions{})
}

func (b *Map[K, V]) Descend(key K) iter.Seq2[K, V] {
	var emptyValue V
	return b.tree.Descend(key, emptyValue)
}

func (b *Map[K, V]) DescendMut(key K) iter.Seq2[K, V] {
	var emptyValue V
	return b.tree.DescendMut(key, emptyValue)
}

func (b *Map[K, V]) IndexOf(key K) (int, bool) {
	var emptyValue V
	return b.tree.IndexOf(key, emptyValue)
}

func (b *Map[K, V]) All() iter.Seq2[K, V] {
	return b.tree.All()
}

func (b *Map[K, V]) AllMut() iter.Seq2[K, V] {
	return b.tree.AllMut()
}

func (b *Map[K, V]) Backward() iter.Seq2[K, V] {
	return b.tree.Backward()
}

func (b *Map[K, V]) BackwardMut() iter.Seq2[K, V] {
	return b.tree.BackwardMut()
}

func (b *Map[K, V]) AscendAt(index int) iter.Seq2[K, V] {
	return b.tree.AscendAt(index)
}

func (b *Map[K, V]) AscendAtMut(index int) iter.Seq2[K, V] {
	return b.tree.AscendAtMut(index)
}

func (b *Map[K, V]) DescendAt(index int) iter.Seq2[K, V] {
	return b.tree.DescendAt(index)
}

func (b *Map[K, V]) DescendAtMut(index int) iter.Seq2[K, V] {
	return b.tree.DescendAtMut(index)
}

func (b *Map[K, V]) PushFront(key K, value V) bool {
	b.init()
	return b.tree.PushFront(key, value)
}

func (b *Map[K, V]) PushBack(key K, value V) bool {
	b.init()
	return b.tree.PushBack(key, value)
}

func (b *Map[K, V]) PopFront() (K, V, bool) {
	return b.tree.PopFront()
}

func (b *Map[K, V]) PopBack() (K, V, bool) {
	return b.tree.PopBack()
}

func (b *Map[K, V]) PopFrontIf(cond func(key K, value V) bool) (K, V, bool) {
	return b.tree.PopFrontIf(cond)
}

func (b *Map[K, V]) PopBackIf(cond func(key K, value V) bool) (K, V, bool) {
	return b.tree.PopBackIf(cond)
}

func (b *Map[K, V]) Front() (K, V, bool) {
	return b.tree.Front()
}

func (b *Map[K, V]) Back() (K, V, bool) {
	return b.tree.Back()
}

func (b *Map[K, V]) FrontMut() (K, V, bool) {
	return b.tree.FrontMut()
}

func (b *Map[K, V]) BackMut() (K, V, bool) {
	return b.tree.BackMut()
}

func (b *Map[K, V]) InsertAt(index int, key K, value V) bool {
	b.init()
	return b.tree.InsertAt(index, key, value)
}

func (b *Map[K, V]) DeleteAt(index int) (K, V, bool) {
	return b.tree.DeleteAt(index)
}

func (b *Map[K, V]) ReplaceAt(index int, key K, value V) (K, V, bool) {
	return b.tree.ReplaceAt(index, key, value)
}

func (b *Map[K, V]) GetAt(index int) (K, V, bool) {
	return b.tree.GetAt(index)
}

func (b *Map[K, V]) GetAtMut(index int) (K, V, bool) {
	return b.tree.GetAtMut(index)
}

func (b *Map[K, V]) Keys() iter.Seq[K] {
	return iterK(b.tree.All())
}

func (b *Map[K, V]) Values() iter.Seq[V] {
	return iterV(b.tree.All())
}

func (b *Map[K, V]) ValuesMut() iter.Seq[V] {
	return iterV(b.tree.AllMut())
}

func (b *Map[K, V]) Seek(key K) (K, V, bool) {
	var emptyValue V
	return b.tree.Seek(key, emptyValue)
}

func (b *Map[K, V]) SeekNext(key K) (K, V, bool) {
	var emptyValue V
	return b.tree.SeekNext(key, emptyValue)
}

func (b *Map[K, V]) SeekPrev(key K) (K, V, bool) {
	var emptyValue V
	return b.tree.SeekPrev(key, emptyValue)
}

func (b *Map[K, V]) SeekMut(key K) (K, V, bool) {
	var emptyValue V
	return b.tree.SeekMut(key, emptyValue)
}

func (b *Map[K, V]) SeekNextMut(key K) (K, V, bool) {
	var emptyValue V
	return b.tree.SeekNextMut(key, emptyValue)
}

func (b *Map[K, V]) SeekPrevMut(key K) (K, V, bool) {
	var emptyValue V
	return b.tree.SeekPrevMut(key, emptyValue)
}

func (b *Map[K, V]) Drain() iter.Seq2[K, V] {
	return b.tree.Drain()
}

func (b *Map[K, V]) DrainBackward() iter.Seq2[K, V] {
	return b.tree.DrainBackward()
}

////////////////////////////////////////////////////////////////////////////////
// Queue
////////////////////////////////////////////////////////////////////////////////

// Queue provides the functionality of a queue - specifically, a
// FIFO (first-in, first-out) data structure.
type Queue[T any] struct {
	tree tree[omit, T]
}

type QueueOptions[T any] struct {
	Copy    func(T) T
	Release func(T)
}

func NewQueueOptions[T any](opts QueueOptions[T]) *Queue[T] {
	b := new(Queue[T])
	b.tree.dataCopy = opts.Copy
	b.tree.dataRelease = opts.Release
	return b
}

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

func (b *Queue[T]) PopIf(cond func(item T) bool) (T, bool) {
	if cond == nil {
		return b.Pop()
	}
	_, item, ok := b.tree.PopFrontIf(func(_ omit, item T) bool {
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
	_, item, ok := b.tree.GetAt(pos)
	return item, ok
}

// AtMut returns the item at position, after first, in queue.
// Returns false if no item is found at position.
// MUTABLE OPERATION.
func (b *Queue[T]) AtMut(pos int) (T, bool) {
	_, item, ok := b.tree.GetAtMut(pos)
	return item, ok
}

// Len returns the number of items in queue
func (b *Queue[T]) Len() int {
	return b.tree.Len()
}

// Copy the queue.
// This is a fast O(1) operation using a copy-on-write method.
func (b *Queue[T]) Copy() *Queue[T] {
	b2 := new(Queue[T])
	*b2 = *b
	b.tree.CopyInto(&b2.tree)
	return b2
}

// Clear the queue.
func (b *Queue[T]) Clear() {
	b.tree.Clear()
}

// Release will clear the queue and release any references.
// This method is functionally equivalent to Clear() but is an optimization for
// collections that are copied using Copy().
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

func (b *Queue[K]) Drain() iter.Seq[K] {
	return iterV(b.tree.Drain())
}

////////////////////////////////////////////////////////////////////////////////
// Set
////////////////////////////////////////////////////////////////////////////////

type Set[K cmp.Ordered] struct {
	base Map[K, omit]
}

func NewSet[K cmp.Ordered]() *Set[K] {
	return new(Set[K])
}

func (b *Set[K]) Insert(key K) bool {
	_, inserted := b.base.Insert(key, omit{})
	return inserted
}

func (b *Set[K]) Delete(key K) bool {
	_, deleted := b.base.Delete(key)
	return deleted
}

func (b *Set[K]) Contains(key K) bool {
	return b.base.Contains(key)
}

func (b *Set[K]) InsertAt(index int, key K) bool {
	return b.base.InsertAt(index, key, omit{})
}

func (b *Set[K]) DeleteAt(index int) (K, bool) {
	key, _, ok := b.base.DeleteAt(index)
	return key, ok
}

func (b *Set[K]) ReplaceAt(index int, key K) (K, bool) {
	key, _, ok := b.base.ReplaceAt(index, key, omit{})
	return key, ok
}

func (b *Set[K]) GetAt(index int) (K, bool) {
	key, _, ok := b.base.GetAt(index)
	return key, ok
}

func (b *Set[K]) IndexOf(key K) (int, bool) {
	return b.base.IndexOf(key)
}

func (b *Set[K]) Ascend(key K) iter.Seq[K] {
	return iterK(b.base.Ascend(key))
}

func (b *Set[K]) Descend(key K) iter.Seq[K] {
	return iterK(b.base.Descend(key))
}

func (b *Set[K]) All() iter.Seq[K] {
	return iterK(b.base.All())
}

func (b *Set[K]) Backward() iter.Seq[K] {
	return iterK(b.base.Backward())
}

func (b *Set[K]) AscendAt(index int) iter.Seq[K] {
	return iterK(b.base.AscendAt(index))
}

func (b *Set[K]) DescendAt(index int) iter.Seq[K] {
	return iterK(b.base.DescendAt(index))
}

func (b *Set[K]) Seek(key K) (K, bool) {
	key, _, ok := b.base.Seek(key)
	return key, ok
}

func (b *Set[K]) SeekNext(key K) (K, bool) {
	key, _, ok := b.base.SeekNext(key)
	return key, ok
}

func (b *Set[K]) SeekPrev(key K) (K, bool) {
	key, _, ok := b.base.SeekPrev(key)
	return key, ok
}

func (b *Set[K]) PushFront(key K) bool {
	return b.base.PushFront(key, omit{})
}

func (b *Set[K]) PushBack(key K) bool {
	return b.base.PushBack(key, omit{})
}

func (b *Set[K]) PopFront() (K, bool) {
	key, _, ok := b.base.PopFront()
	return key, ok
}

func (b *Set[K]) PopBack() (K, bool) {
	key, _, ok := b.base.PopBack()
	return key, ok
}

func (b *Set[K]) PopFrontIf(cond func(key K) bool) (K, bool) {
	if cond == nil {
		return b.PopFront()
	}
	key, _, ok := b.base.PopFrontIf(func(key K, _ omit) bool {
		return cond(key)
	})
	return key, ok
}

func (b *Set[K]) PopBackIf(cond func(key K) bool) (K, bool) {
	if cond == nil {
		return b.PopBack()
	}
	key, _, ok := b.base.PopBackIf(func(key K, _ omit) bool {
		return cond(key)
	})
	return key, ok
}

func (b *Set[K]) Front() (K, bool) {
	key, _, ok := b.base.Front()
	return key, ok
}

func (b *Set[K]) Back() (K, bool) {
	key, _, ok := b.base.Back()
	return key, ok
}

func (b *Set[K]) Len() int {
	return b.base.Len()
}

func (b *Set[K]) Clear() {
	b.base.Clear()
}

func (b *Set[K]) Copy() *Set[K] {
	b2 := new(Set[K])
	*b2 = *b
	b.base.copyInto(&b2.base)
	return b2
}

func (b *Set[K]) Release() {
	b.base.Release()
}

func (b *Set[K]) DeleteRangeAtOptions(index, count int, opts DeleteRangeOptions,
) Slice[K] {
	return sliceK(b.base.DeleteRangeAtOptions(index, count, opts))
}

func (b *Set[K]) DeleteRangeOptions(min, max K, opts DeleteRangeOptions,
) Slice[K] {
	return sliceK(b.base.DeleteRangeOptions(min, max, opts))
}

func (b *Set[K]) DeleteRangeAt(index, count int) Slice[K] {
	return b.DeleteRangeAtOptions(index, count, DeleteRangeOptions{})
}

func (b *Set[K]) DeleteRange(min, max K) Slice[K] {
	return b.DeleteRangeOptions(min, max, DeleteRangeOptions{})
}

func (b *Set[K]) Drain() iter.Seq[K] {
	return iterK(b.base.Drain())
}

func (b *Set[K]) DrainBackward() iter.Seq[K] {
	return iterK(b.base.DrainBackward())
}

////////////////////////////////////////////////////////////////////////////////
// Stack
////////////////////////////////////////////////////////////////////////////////

// Stack provides the functionality of a stack - specifically, a
// LIFO (last-in, first-out) data structure.
type Stack[T any] struct {
	tree tree[omit, T]
}

type StackOptions[T any] struct {
	Copy    func(T) T
	Release func(T)
}

func NewStackOptions[T any](opts StackOptions[T]) *Stack[T] {
	b := new(Stack[T])
	b.tree.dataCopy = opts.Copy
	b.tree.dataRelease = opts.Release
	return b
}

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
	_, item, ok := b.tree.GetAt(b.tree.Len() - pos - 1)
	return item, ok
}

// AtMut returns the item at position, after top, in Stack.
// Returns false if no item is found at position.
// MUTABLE OPERATION.
func (b *Stack[T]) AtMut(pos int) (T, bool) {
	_, item, ok := b.tree.GetAtMut(b.tree.Len() - pos - 1)
	return item, ok
}

// Len returns the number of items in stack.
func (b *Stack[T]) Len() int {
	return b.tree.Len()
}

// Copy the stack.
// This is a fast O(1) operation using a copy-on-write method.
func (b *Stack[T]) Copy() *Stack[T] {
	b2 := new(Stack[T])
	*b2 = *b
	b.tree.CopyInto(&b2.tree)
	return b2
}

// Clear the stack.
func (b *Stack[T]) Clear() {
	b.tree.Clear()
}

// Release will clear the stack and releases any copied reference.
// This method is functionally equivalent to Clear() but is an optimization for
// collections that are copied using Copy().
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

////////////////////////////////////////////////////////////////////////////////
// Table
////////////////////////////////////////////////////////////////////////////////

type Table[T any] struct {
	tree tree[omit, T]
}

type TableOptions[T any] struct {
	Compare func(T, T) int
	Less    func(T, T) bool
	Copy    func(T) T
	Release func(T)
}

func NewTableOptions[T any](opts TableOptions[T]) *Table[T] {
	b := new(Table[T])
	if opts.Compare != nil {
		b.initCompare(opts.Compare)
	} else if opts.Less != nil {
		b.initLess(opts.Less)
	}
	b.tree.dataCopy = opts.Copy
	b.tree.dataRelease = opts.Release
	b.init()
	return b
}

func NewTable[T any]() *Table[T] {
	return NewTableOptions(TableOptions[T]{})
}

func (b *Table[T]) initCompare(compare func(T, T) int) {
	b.tree.dataCompare = func(_ omit, a T, _ omit, b T) int {
		return compare(a, b)
	}
	b.tree.dataSearch = func(len int, _ *omit, p *T, _ omit, key T,
	) (i int, found bool) {
		items := unsafe.Slice(p, len)
		if compare(key, items[len-1]) > 0 {
			return len, false
		}
		low, high := 0, len
		for low < high {
			h := (low + high) >> 1
			c := compare(key, items[h])
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
}

func (b *Table[T]) initLess(less func(T, T) bool) {
	b.tree.dataCompare = func(_ omit, a T, _ omit, b T) int {
		if less(a, b) {
			return -1
		} else if less(b, a) {
			return 1
		} else {
			return 0
		}
	}
	b.tree.dataSearch = func(len int, _ *omit, p *T, _ omit, key T,
	) (i int, found bool) {
		items := unsafe.Slice(p, len)
		if less(items[len-1], key) {
			return len, false
		}
		low, high := 0, len
		for low < high {
			h := (low + high) >> 1
			if !less(key, items[h]) {
				low = h + 1
			} else {
				high = h
			}
		}
		if low > 0 && !less(items[low-1], key) {
			return low - 1, true
		}
		return low, false
	}
}

//go:noinline
func (b *Table[T]) init0() {
	if b.tree.dataCompare == nil {
		c := compareFor[T]()
		if c.ok {
			b.tree.dataCompare = c.compare
			b.tree.dataSearch = c.search
		}
	}
	b.tree.initd = true
}

func (b *Table[T]) init() bool {
	// Called from Insert, InsertAt, Set, PushFront, PushBack
	if !b.tree.initd {
		b.init0()
	}
	return b.tree.dataCompare != nil
}

func (b *Table[T]) Insert(item T) (T, bool) {
	var empty T
	if !b.init() {
		return empty, false
	}
	return b.tree.Insert(omit{}, item)
}

func (b *Table[T]) Replace(item T) (T, bool) {
	return b.tree.Replace(omit{}, item)
}

func (b *Table[T]) Set(item T) (T, bool) {
	var empty T
	if !b.init() {
		return empty, false
	}
	return b.tree.Set(omit{}, item)
}

func (b *Table[T]) Contains(key T) bool {
	return b.tree.Contains(omit{}, key)
}

func (b *Table[T]) Get(key T) (T, bool) {
	return b.tree.Get(omit{}, key)
}

func (b *Table[T]) GetMut(key T) (T, bool) {
	return b.tree.GetMut(omit{}, key)
}

func (b *Table[T]) Delete(key T) (T, bool) {
	old, deleted := b.tree.Delete(omit{}, key)
	return old, deleted
}

func (b *Table[T]) All() iter.Seq[T] {
	return iterV(b.tree.All())
}

func (b *Table[T]) AllMut() iter.Seq[T] {
	return iterV(b.tree.AllMut())
}

func (b *Table[T]) Ascend(pivot T) iter.Seq[T] {
	return iterV(b.tree.Ascend(omit{}, pivot))
}

func (b *Table[T]) AscendMut(pivot T) iter.Seq[T] {
	return iterV(b.tree.AscendMut(omit{}, pivot))
}

func (b *Table[T]) Descend(pivot T) iter.Seq[T] {
	return iterV(b.tree.Descend(omit{}, pivot))
}

func (b *Table[T]) DescendMut(pivot T) iter.Seq[T] {
	return iterV(b.tree.DescendMut(omit{}, pivot))
}

func (b *Table[T]) Backward() iter.Seq[T] {
	return iterV(b.tree.Backward())
}

func (b *Table[T]) BackwardMut() iter.Seq[T] {
	return iterV(b.tree.BackwardMut())
}

func (b *Table[T]) PushFront(item T) bool {
	if !b.init() {
		return false
	}
	return b.tree.PushFront(omit{}, item)
}

func (b *Table[T]) PushBack(item T) bool {
	if !b.init() {
		return false
	}
	return b.tree.PushBack(omit{}, item)
}

func (b *Table[T]) Front() (T, bool) {
	_, value, ok := b.tree.Front()
	return value, ok
}

func (b *Table[T]) FrontMut() (T, bool) {
	_, value, ok := b.tree.FrontMut()
	return value, ok
}

func (b *Table[T]) Back() (T, bool) {
	_, value, ok := b.tree.Back()
	return value, ok
}

func (b *Table[T]) BackMut() (T, bool) {
	_, value, ok := b.tree.BackMut()
	return value, ok
}

func (b *Table[T]) PopFront() (T, bool) {
	_, value, ok := b.tree.PopFront()
	return value, ok
}

func (b *Table[T]) PopBack() (T, bool) {
	_, value, ok := b.tree.PopBack()
	return value, ok
}

func (b *Table[T]) PopFrontIf(cond func(item T) bool) (T, bool) {
	if cond == nil {
		return b.PopFront()
	}
	_, value, ok := b.tree.PopFrontIf(func(_ omit, item T) bool {
		return cond(item)
	})
	return value, ok
}

func (b *Table[T]) PopBackIf(cond func(item T) bool) (T, bool) {
	if cond == nil {
		return b.PopBack()
	}
	_, value, ok := b.tree.PopBackIf(func(_ omit, item T) bool {
		return cond(item)
	})
	return value, ok
}

func (b *Table[T]) Seek(key T) (T, bool) {
	_, value, ok := b.tree.Seek(omit{}, key)
	return value, ok
}

func (b *Table[T]) SeekMut(key T) (T, bool) {
	_, value, ok := b.tree.SeekMut(omit{}, key)
	return value, ok
}

func (b *Table[T]) SeekNext(key T) (T, bool) {
	_, value, ok := b.tree.SeekNext(omit{}, key)
	return value, ok
}

func (b *Table[T]) SeekNextMut(key T) (T, bool) {
	_, value, ok := b.tree.SeekNextMut(omit{}, key)
	return value, ok
}

func (b *Table[T]) SeekPrev(key T) (T, bool) {
	_, value, ok := b.tree.SeekPrev(omit{}, key)
	return value, ok
}

func (b *Table[T]) SeekPrevMut(key T) (T, bool) {
	_, value, ok := b.tree.SeekPrevMut(omit{}, key)
	return value, ok
}

func (b *Table[T]) InsertAt(index int, item T) bool {
	b.init()
	return b.tree.InsertAt(index, omit{}, item)
}

func (b *Table[T]) ReplaceAt(index int, item T) (T, bool) {
	_, value, ok := b.tree.ReplaceAt(index, omit{}, item)
	return value, ok
}

func (b *Table[T]) DeleteAt(index int) (T, bool) {
	_, value, ok := b.tree.DeleteAt(index)
	return value, ok
}

func (b *Table[T]) GetAt(index int) (T, bool) {
	_, value, ok := b.tree.GetAt(index)
	return value, ok
}

func (b *Table[T]) GetAtMut(index int) (T, bool) {
	_, value, ok := b.tree.GetAtMut(index)
	return value, ok
}

func (b *Table[T]) IndexOf(key T) (int, bool) {
	return b.tree.IndexOf(omit{}, key)
}

func (b *Table[T]) AscendAt(index int) iter.Seq[T] {
	return iterV(b.tree.AscendAt(index))
}

func (b *Table[T]) DescendAt(index int) iter.Seq[T] {
	return iterV(b.tree.DescendAt(index))
}

func (b *Table[T]) AscendAtMut(index int) iter.Seq[T] {
	return iterV(b.tree.AscendAtMut(index))
}

func (b *Table[T]) DescendAtMut(index int) iter.Seq[T] {
	return iterV(b.tree.DescendAtMut(index))
}

func (b *Table[T]) DeleteRangeAtOptions(index, count int,
	opts DeleteRangeOptions,
) Slice[T] {
	return sliceV(b.tree.DeleteRangeAt(index, count, opts))
}

func (b *Table[T]) DeleteRangeOptions(min, max T, opts DeleteRangeOptions,
) Slice[T] {
	return sliceV(b.tree.DeleteRange(omit{}, min, omit{}, max, opts))
}

func (b *Table[T]) DeleteRangeAt(index, count int) Slice[T] {
	return b.DeleteRangeAtOptions(index, count, DeleteRangeOptions{})
}

func (b *Table[T]) DeleteRange(min, max T) Slice[T] {
	return b.DeleteRangeOptions(min, max, DeleteRangeOptions{})
}

func (b *Table[T]) Len() int {
	return b.tree.Len()
}

func (b *Table[T]) Clear() {
	b.tree.Clear()
}

func (b *Table[T]) Release() {
	b.tree.Release()
}

func (b *Table[T]) Copy() *Table[T] {
	b2 := new(Table[T])
	*b2 = *b
	b.tree.CopyInto(&b2.tree)
	return b2
}

func (b *Table[T]) Drain() iter.Seq[T] {
	return iterV(b.tree.Drain())
}

func (b *Table[T]) DrainBackward() iter.Seq[T] {
	return iterV(b.tree.DrainBackward())
}

////////////////////////////////////////////////////////////////////////////////
// table compare detection
////////////////////////////////////////////////////////////////////////////////

type ctype[T any] struct {
	ok      bool
	idr     bool
	fields  []cfield
	compare func(omit, T, omit, T) int
	search  func(int, *omit, *T, omit, T) (int, bool)
}

// direct converts type T to type O.
// Type T _MUST_ be exactly type O.
func direct[T, O any](x T) O {
	return *(*O)(unsafe.Pointer(&x))
}

// indirect converts type *T to type O.
// Type *T _MUST_ be exactly type O.
func indirect[T, O any](x T) O {
	return *(*O)(*(*unsafe.Pointer)(unsafe.Pointer(&x)))
}

// directAt accesses an object at array index, and onverts type T to type O.
// Type T _MUST_ be exactly type O.
func directAt[T, O any](p *T, z int, i int) (o O) {
	return *(*O)(unsafe.Add(unsafe.Pointer(p), i*z))
}

func directOffset[T, O any](t T, offset uintptr) (o O) {
	return *(*O)(unsafe.Add(unsafe.Pointer(&t), offset))
}

func directOffsetAt[T, O any](p *T, z uintptr, i int, offset uintptr) (o O) {
	return *(*O)(unsafe.Add(unsafe.Pointer(p), uintptr(i)*z+offset))
}

func indirectOffset[T, O any](t T, offset uintptr) (o O) {
	return *(*O)(unsafe.Add(*(*unsafe.Pointer)(unsafe.Pointer(&t)), offset))
}

func compareOrderedD[T any, O cmp.Ordered](_ omit, a T, _ omit, b T) int {
	return cmp.Compare(direct[T, O](a), direct[T, O](b))
}

func compareOrderedI[T any, O cmp.Ordered](_ omit, a T, _ omit, b T) int {
	return cmp.Compare(indirect[T, O](a), indirect[T, O](b))
}

func lessOrderedI[T any, O cmp.Ordered](a, b T) bool {
	return indirect[T, O](a) < indirect[T, O](b)
}

func bsearchOrderedI[T any, O cmp.Ordered](len int, _ *omit, p *T, _ omit,
	key T,
) (i int, found bool) {
	items := unsafe.Slice(p, len)
	if lessOrderedI[T, O](items[len-1], key) {
		return len, false
	}
	low, high := 0, len
	for low < high {
		h := (low + high) >> 1
		if !lessOrderedI[T, O](key, items[h]) {
			low = h + 1
		} else {
			high = h
		}
	}
	if low > 0 && !lessOrderedI[T, O](items[low-1], key) {
		return low - 1, true
	}
	return low, false
}

func bsearchOrderedD[T any, O cmp.Ordered](len int, _ *omit, p *T, _ omit,
	vkey T,
) (i int, found bool) {
	z := int(unsafe.Sizeof(vkey))
	key := direct[T, O](vkey)
	if key > directAt[T, O](p, z, len-1) {
		return len, false
	}
	low, high := 0, len
	for low < high {
		h := (low + high) >> 1
		if !(key < directAt[T, O](p, z, h)) {
			low = h + 1
		} else {
			high = h
		}
	}
	if low > 0 && !(directAt[T, O](p, z, low-1) < key) {
		return low - 1, true
	}
	return low, false
}

func lsearchOrderedD[T any, O cmp.Ordered](len int, _ *omit, p *T, _ omit,
	vkey T,
) (i int, found bool) {
	z := int(unsafe.Sizeof(vkey))
	key := direct[T, O](vkey)
	if key > directAt[T, O](p, z, len-1) {
		return len, false
	}
	for key > directAt[T, O](p, z, i) {
		i++
	}
	return i, key == directAt[T, O](p, z, i)
}

func lsearchFieldsOrderedD[T any, O cmp.Ordered](len int, p *T, vkey T,
	offset uintptr, fields []cfield,
) (i int, found bool) {
	items := unsafe.Slice(p, len)
	z := unsafe.Sizeof(vkey)
	key := directOffset[T, O](vkey, offset)
	c := cmp.Compare(key, directOffsetAt[T, O](p, z, len-1, offset))
	if c == 0 {
		c = compareFields(vkey, items[len-1], fields, false)
	}
	if c > 0 {
		return len, false
	}
	for {
		for key > directOffsetAt[T, O](p, z, i, offset) {
			i++
		}
		if key == directOffsetAt[T, O](p, z, i, offset) {
			c := compareFields(vkey, items[i], fields, false)
			if c > 0 {
				i++
				continue
			}
			return i, c == 0
		}
		return i, false
	}
}

func bsearchFieldsOrderedD[T any, O cmp.Ordered](len0 int, p *T, vkey T,
	offset uintptr, fields []cfield, dir cdir,
) (int, bool) {
	// expected len(fields) > 0
	items := unsafe.Slice(p, len0)
	key := directOffset[T, O](vkey, offset)
	low, high := 0, len(items)
	for low < high {
		h := (low + high) >> 1
		c := cmp.Compare(key, directOffset[T, O](items[h], offset))
		c *= int(dir)
		if c < 0 {
			high = h
		} else if c > 0 {
			low = h + 1
		} else {
			c := compareFields(vkey, items[h], fields, false)
			if c < 0 {
				high = h
			} else if c > 0 {
				low = h + 1
			} else {
				return h, true
			}
		}
	}
	return low, false
}

func bsearchFieldsOrderedI[T any, O cmp.Ordered](len0 int, p *T, vkey T,
	offset uintptr, fields []cfield, dir cdir,
) (int, bool) {
	// expected len(fields) > 0
	items := unsafe.Slice(p, len0)
	key := indirectOffset[T, O](vkey, offset)
	low, high := 0, len(items)
	for low < high {
		h := (low + high) >> 1
		c := cmp.Compare(key, indirectOffset[T, O](items[h], offset))
		c *= int(dir)
		if c < 0 {
			high = h
		} else if c > 0 {
			low = h + 1
		} else {
			c := compareFields(vkey, items[h], fields, true)
			if c < 0 {
				high = h
			} else if c > 0 {
				low = h + 1
			} else {
				return h, true
			}
		}
	}
	return low, false
}

func bsearchNoFieldsOrderedI[T any, O cmp.Ordered](len0 int, p *T, vkey T,
	offset uintptr, dir cdir,
) (int, bool) {
	items := unsafe.Slice(p, len0)
	key := indirectOffset[T, O](vkey, offset)
	low, high := 0, len(items)
	for low < high {
		h := (low + high) >> 1
		c := cmp.Compare(key, indirectOffset[T, O](items[h], offset))
		c *= int(dir)
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

func bsearchNoFieldsOrderedD[T any, O cmp.Ordered](len0 int, p *T, vkey T,
	offset uintptr, dir cdir,
) (int, bool) {
	items := unsafe.Slice(p, len0)
	key := directOffset[T, O](vkey, offset)
	low, high := 0, len(items)
	for low < high {
		h := (low + high) >> 1
		c := cmp.Compare(key, directOffset[T, O](items[h], offset))
		c *= int(dir)
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

func lsearchNoFieldsOrderedD[T any, O cmp.Ordered](len int, p *T, vkey T,
	offset uintptr,
) (i int, found bool) {
	z := unsafe.Sizeof(vkey)
	key := directOffset[T, O](vkey, offset)
	if key > directOffsetAt[T, O](p, z, len-1, offset) {
		return len, false
	}
	for key > directOffsetAt[T, O](p, z, i, offset) {
		i++
	}
	return i, key == directOffsetAt[T, O](p, z, i, offset)
}

func bsearchFieldsTypeI[T any](len0 int, p *T, vkey T, fields []cfield,
) (int, bool) {
	items := unsafe.Slice(p, len0)
	low, high := 0, len(items)
	for low < high {
		h := (low + high) >> 1
		c := compareFields(vkey, items[h], fields, true)
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

func bsearchFieldsTypeD[T any](len0 int, p *T, vkey T, fields []cfield,
) (int, bool) {
	items := unsafe.Slice(p, len0)
	low, high := 0, len(items)
	for low < high {
		h := (low + high) >> 1
		c := compareFields(vkey, items[h], fields, false)
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

type cdir int8

const (
	cdesc cdir = -1
	casc  cdir = 1
)

type cfield struct {
	index   int
	dir     cdir
	name    string
	offset  uintptr
	collate ccollate
	kind    reflect.Kind // used kind
}

func compareTypeI[T any, O cmp.Ordered](ta, tb T, offset uintptr, dir cdir,
) int {
	a := *(*O)(unsafe.Add(*(*unsafe.Pointer)(unsafe.Pointer(&ta)), offset))
	b := *(*O)(unsafe.Add(*(*unsafe.Pointer)(unsafe.Pointer(&tb)), offset))
	return cmp.Compare(a, b) * int(dir)
}

func compareTypeD[T any, O cmp.Ordered](ta, tb T, offset uintptr, dir cdir,
) int {
	a := *(*O)(unsafe.Add(unsafe.Pointer(&ta), offset))
	b := *(*O)(unsafe.Add(unsafe.Pointer(&tb), offset))
	return cmp.Compare(a, b) * int(dir)
}

func compareType[T any, C cmp.Ordered](ta, tb T, offset uintptr, dir cdir,
	idr bool,
) int {
	if idr {
		return compareTypeI[T, C](ta, tb, offset, dir)
	} else {
		return compareTypeD[T, C](ta, tb, offset, dir)
	}
}

func compareBinCS(a, b string) int {
	return cmp.Compare(a, b)
}

func compareBinCI(a, b string) int {
	n := min(len(a), len(b))
	for i := range n {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 32
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 32
		}
		if ca < cb {
			return -1
		} else if ca > cb {
			return 1
		}
	}
	return cmp.Compare(len(a), len(b))
}

func compareUtf8(a, b string, ci bool) int {
	for {
		ca, za := utf8.DecodeRuneInString(a)
		cb, zb := utf8.DecodeRuneInString(b)
		if za == 0 {
			if zb == 0 {
				return 0
			} else {
				return -1
			}
		} else if zb == 0 {
			return 1
		}
		if ci {
			ca = unicode.ToLower(ca)
			cb = unicode.ToLower(cb)
		}
		if ca < cb {
			return -1
		} else if ca > cb {
			return 1
		}
		a = a[za:]
		b = b[zb:]
	}
}

func compareUtf8CS(a, b string) int {
	n := min(len(a), len(b))
	i := 0
	for i = range n {
		ca, cb := a[i], b[i]
		if ca >= utf8.RuneSelf || cb >= utf8.RuneSelf {
			return compareUtf8(a[i:], b[i:], false)
		}
		if ca < cb {
			return -1
		} else if ca > cb {
			return 1
		}
	}
	return cmp.Compare(len(a), len(b))
}

func compareUtf8CI(a, b string) int {
	n := min(len(a), len(b))
	i := 0
	for i = range n {
		ca, cb := a[i], b[i]
		if ca >= utf8.RuneSelf || cb >= utf8.RuneSelf {
			return compareUtf8(a[i:], b[i:], true)
		}
		if ca >= 'A' && ca <= 'Z' {
			ca += 32
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 32
		}
		if ca < cb {
			return -1
		} else if ca > cb {
			return 1
		}
	}
	return cmp.Compare(len(a), len(b))
}

func compareStringCollate(a, b string, collate ccollate) int {
	switch collate {
	default: // case colBinCS:
		return compareBinCS(a, b)
	case cbinci:
		return compareBinCI(a, b)
	case cutf8cs:
		return compareUtf8CS(a, b)
	case cutf8ci:
		return compareUtf8CI(a, b)
	}
}

func compareStringI[T any](ta, tb T, offset uintptr, collate ccollate, dir cdir,
) int {
	a := *(*string)(unsafe.Add(*(*unsafe.Pointer)(unsafe.Pointer(&ta)), offset))
	b := *(*string)(unsafe.Add(*(*unsafe.Pointer)(unsafe.Pointer(&tb)), offset))
	return compareStringCollate(a, b, collate) * int(dir)
}

func compareStringD[T any](ta, tb T, offset uintptr, collate ccollate, dir cdir,
) int {
	a := *(*string)(unsafe.Add(unsafe.Pointer(&ta), offset))
	b := *(*string)(unsafe.Add(unsafe.Pointer(&tb), offset))
	return compareStringCollate(a, b, collate) * int(dir)
}

func compareString[T any](ta, tb T, offset uintptr, collate ccollate, dir cdir,
	idr bool,
) int {
	if idr {
		return compareStringI(ta, tb, offset, collate, dir)
	} else {
		return compareStringD(ta, tb, offset, collate, dir)
	}
}

func compareField[T any](a, b T, kind reflect.Kind, offset uintptr,
	collate ccollate, dir cdir, idr bool,
) int {
	var c int
	switch kind {
	case reflect.Int:
		c = compareType[T, int](a, b, offset, dir, idr)
	case reflect.Int8:
		c = compareType[T, int8](a, b, offset, dir, idr)
	case reflect.Int16:
		c = compareType[T, int16](a, b, offset, dir, idr)
	case reflect.Int32:
		c = compareType[T, int32](a, b, offset, dir, idr)
	case reflect.Int64:
		c = compareType[T, int64](a, b, offset, dir, idr)
	case reflect.Uint:
		c = compareType[T, uint](a, b, offset, dir, idr)
	case reflect.Uint8:
		c = compareType[T, uint8](a, b, offset, dir, idr)
	case reflect.Uint16:
		c = compareType[T, uint16](a, b, offset, dir, idr)
	case reflect.Uint32:
		c = compareType[T, uint32](a, b, offset, dir, idr)
	case reflect.Uint64:
		c = compareType[T, uint64](a, b, offset, dir, idr)
	case reflect.Uintptr:
		c = compareType[T, uintptr](a, b, offset, dir, idr)
	case reflect.Float32:
		c = compareType[T, float32](a, b, offset, dir, idr)
	case reflect.Float64:
		c = compareType[T, float64](a, b, offset, dir, idr)
	case reflect.String:
		c = compareString(a, b, offset, collate, dir, idr)
	}
	return c
}

func compareFields[T any](a, b T, fields []cfield, idr bool) int {
	var c int
	for i := range fields {
		c = compareField(a, b, fields[i].kind, fields[i].offset,
			fields[i].collate, fields[i].dir, idr)
		if c != 0 {
			break
		}
	}
	return c
}

type ccollate byte

const (
	cbincs  ccollate = iota // binary string -- case sensitive
	cbinci                  // binary string -- compare insensitive
	cutf8cs                 // unicode (utf8) compare -- case sensitive
	cutf8ci                 // unicode (utf8) compare -- case insensitive
)

func bsearchFieldsOrderedType[T any, O cmp.Ordered](fields []cfield,
	offset uintptr, dir cdir, idr bool,
) func(int, *omit, *T, omit, T) (int, bool) {
	if idr {
		return func(len int, _ *omit, p *T, _ omit, key T) (int, bool) {
			return bsearchFieldsOrderedI[T, O](len, p, key, offset, fields, dir)
		}
	} else {
		return func(len int, _ *omit, p *T, _ omit, key T) (int, bool) {
			return bsearchFieldsOrderedD[T, O](len, p, key, offset, fields, dir)
		}
	}
}

func lsearchFieldsOrderedType[T any, O cmp.Ordered](fields []cfield,
	offset uintptr,
) func(int, *omit, *T, omit, T) (int, bool) {
	return func(len0 int, _ *omit, p *T, _ omit, key T) (int, bool) {
		return lsearchFieldsOrderedD[T, O](len0, p, key, offset, fields)
	}
}

func bsearchNoFieldsOrderedType[T any, O cmp.Ordered](offset uintptr, dir cdir,
	idr bool,
) func(int, *omit, *T, omit, T) (int, bool) {
	if idr {
		return func(len int, _ *omit, p *T, _ omit, key T) (int, bool) {
			return bsearchNoFieldsOrderedI[T, O](len, p, key, offset, dir)
		}
	} else {
		return func(len int, _ *omit, p *T, _ omit, key T) (int, bool) {
			return bsearchNoFieldsOrderedD[T, O](len, p, key, offset, dir)
		}
	}
}

func lsearchNoFieldsOrderedType[T any, O cmp.Ordered](offset uintptr,
) func(int, *omit, *T, omit, T) (int, bool) {
	// assumed direct access and asc (idr=false) (dir=fasc)
	return func(len int, _ *omit, p *T, _ omit, key T) (int, bool) {
		return lsearchNoFieldsOrderedD[T, O](len, p, key, offset)
	}
}

func bsearchFieldsType[T any](fields []cfield, idr bool,
) func(len0 int, _ *omit, p *T, _ omit, vkey T) (int, bool) {
	if idr {
		return func(len int, _ *omit, p *T, _ omit, key T) (int, bool) {
			return bsearchFieldsTypeI(len, p, key, fields)
		}
	} else {
		return func(len int, _ *omit, p *T, _ omit, key T) (int, bool) {
			return bsearchFieldsTypeD(len, p, key, fields)
		}
	}
}

func compareForFieldsType[T any, O cmp.Ordered](c *ctype[T], fields []cfield,
	bsearch, idr bool,
) {
	offset := fields[0].offset
	collate := fields[0].collate
	dir := fields[0].dir
	if collate != 0 {
		c.compare = func(_ omit, a T, _ omit, b T) int {
			return compareFields(a, b, fields, idr)
		}
		c.search = bsearchFieldsType[T](fields, idr)
		return
	}
	if len(fields) > 1 {
		c.compare = func(_ omit, a T, _ omit, b T) int {
			return compareFields(a, b, fields, idr)
		}
		if idr || bsearch || dir != casc {
			c.search = bsearchFieldsOrderedType[T, O](fields[1:], offset, dir,
				idr)
		} else {
			c.search = lsearchFieldsOrderedType[T, O](fields[1:], offset)
		}
	} else {
		c.compare = func(_ omit, a T, _ omit, b T) int {
			return compareType[T, O](a, b, offset, dir, idr)
		}
		if idr || bsearch || dir != casc {
			c.search = bsearchNoFieldsOrderedType[T, O](offset, dir, idr)
		} else {
			c.search = lsearchNoFieldsOrderedType[T, O](offset)
		}
	}
}

func compareForFields[T any](c *ctype[T], fields []cfield, bsearch, idr bool) {
	if fields[0].offset == 0 && len(fields) == 1 && fields[0].dir == casc &&
		fields[0].collate == 0 {
		// Single field, offset zero, asc, no collation.
		compareForSimpleType(c, fields[0].kind, bsearch, idr)
		return
	}
	switch fields[0].kind {
	case reflect.Int:
		compareForFieldsType[T, int](c, fields, bsearch, idr)
	case reflect.Int8:
		compareForFieldsType[T, int8](c, fields, bsearch, idr)
	case reflect.Int16:
		compareForFieldsType[T, int16](c, fields, bsearch, idr)
	case reflect.Int32:
		compareForFieldsType[T, int32](c, fields, bsearch, idr)
	case reflect.Int64:
		compareForFieldsType[T, int64](c, fields, bsearch, idr)
	case reflect.Uint:
		compareForFieldsType[T, uint](c, fields, bsearch, idr)
	case reflect.Uint8:
		compareForFieldsType[T, uint8](c, fields, bsearch, idr)
	case reflect.Uint16:
		compareForFieldsType[T, uint16](c, fields, bsearch, idr)
	case reflect.Uint32:
		compareForFieldsType[T, uint32](c, fields, bsearch, idr)
	case reflect.Uint64:
		compareForFieldsType[T, uint64](c, fields, bsearch, idr)
	case reflect.Uintptr:
		compareForFieldsType[T, uintptr](c, fields, bsearch, idr)
	case reflect.Float32:
		compareForFieldsType[T, float32](c, fields, bsearch, idr)
	case reflect.Float64:
		compareForFieldsType[T, float64](c, fields, bsearch, idr)
	case reflect.String:
		compareForFieldsType[T, string](c, fields, true, idr)
	}
}

type gather struct {
	fields []cfield
	first  cfield
	ok     bool
}

func gatherFieldsForStruct(g gather, t reflect.Type, offset uintptr) gather {
	nfields := t.NumField()
	for i := range nfields {
		f := t.Field(i)
		kind := f.Type.Kind()
		if kind == reflect.Struct {
			g = gatherFieldsForStruct(g, f.Type, f.Offset)
			continue
		}
		tags := strings.Split(f.Tag.Get("btree"), ",")
		var key bool
		var dir cdir = casc
		var idx int
		var field cfield
		var collate ccollate
		for _, tag := range tags {
			tag = strings.ToLower(tag)
			if tag == "key" {
				key = true
				idx = math.MaxUint16 + 1
			} else if strings.HasPrefix(tag, "key.") {
				key = true
				x, err := strconv.Atoi(tag[4:])
				if err != nil {
					idx = math.MaxUint16 + 1
				} else if x < 0 {
					idx = 0
				} else if x > math.MaxUint16 {
					idx = math.MaxUint16
				} else {
					idx = x
				}
			} else if tag == "desc" {
				dir = cdesc
			} else if tag == "asc" {
				dir = casc
			} else {
				switch tag {
				case "binary_cs", "bin_cs", "cs":
					collate = cbincs
				case "binary_ci", "bin_ci", "ci":
					collate = cbinci
				case "utf8_cs":
					collate = cutf8cs
				case "utf8_ci":
					collate = cutf8ci
				}
			}
		}
		switch kind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
			reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16,
			reflect.Uint32, reflect.Uint64, reflect.Uintptr,
			reflect.Float32, reflect.Float64, reflect.String:
			field = cfield{
				index:   idx,
				dir:     dir,
				name:    f.Name,
				offset:  offset + f.Offset,
				kind:    kind,
				collate: collate,
			}
			if !g.ok {
				g.first = field
				g.ok = true
			}
		}
		if key {
			g.fields = append(g.fields, field)
		}
	}
	return g
}

func compareForStruct[T any](c *ctype[T], t reflect.Type, bsearch, idr bool) {
	g := gatherFieldsForStruct(gather{}, t, 0)
	fields, first, fok := g.fields, g.first, g.ok
	if len(fields) == 0 {
		if fok {
			fields = append(fields, first)
		} else {
			return
		}
	}
	slices.SortStableFunc(fields, func(a, b cfield) int {
		return cmp.Compare(a.index, b.index)
	})
	for i := 0; i < len(fields); i++ {
		fields[i].index = i
	}
	compareForFields(c, fields, bsearch || t.Size() > 64, idr)
	if c.compare != nil {
		c.fields = fields
	}
}

func compareFuncForType[T any, O cmp.Ordered](c *ctype[T], bsearch,
	idr bool,
) {
	if idr {
		c.compare = compareOrderedI[T, O]
		c.search = bsearchOrderedI[T, O]
	} else {
		c.compare = compareOrderedD[T, O]
		if bsearch {
			c.search = bsearchOrderedD[T, O]
		} else {
			c.search = lsearchOrderedD[T, O]
		}
	}
}

func compareForSimpleType[T any](c *ctype[T], kind reflect.Kind,
	bsearch, idr bool,
) {
	switch kind {
	case reflect.Int:
		compareFuncForType[T, int](c, bsearch, idr)
	case reflect.Int8:
		compareFuncForType[T, int8](c, bsearch, idr)
	case reflect.Int16:
		compareFuncForType[T, int16](c, bsearch, idr)
	case reflect.Int32:
		compareFuncForType[T, int32](c, bsearch, idr)
	case reflect.Int64:
		compareFuncForType[T, int64](c, bsearch, idr)
	case reflect.Uint:
		compareFuncForType[T, uint](c, bsearch, idr)
	case reflect.Uint8:
		compareFuncForType[T, uint8](c, bsearch, idr)
	case reflect.Uint16:
		compareFuncForType[T, uint16](c, bsearch, idr)
	case reflect.Uint32:
		compareFuncForType[T, uint32](c, bsearch, idr)
	case reflect.Uint64:
		compareFuncForType[T, uint64](c, bsearch, idr)
	case reflect.Uintptr:
		compareFuncForType[T, uintptr](c, bsearch, idr)
	case reflect.Float32:
		compareFuncForType[T, float32](c, bsearch, idr)
	case reflect.Float64:
		compareFuncForType[T, float64](c, bsearch, idr)
	case reflect.String:
		compareFuncForType[T, string](c, true, idr)
	}
}

func compareForType[T any](c *ctype[T], t reflect.Type, bsearch bool) {
	compareForSimpleType(c, t.Kind(), bsearch, false)
}

func gettypeid[T any]() uintptr {
	var empty T
	x := any(empty)
	return *(*uintptr)(unsafe.Pointer(&x))
}

// https://zimbry.blogspot.com/2011/09/better-bit-mixing-improving-on.html
// hash u64 using mix13
func mix13(key uint64) uint64 {
	key ^= key >> 30
	key *= 0xbf58476d1ce4e5b9
	key ^= key >> 27
	key *= 0x94d049bb133111eb
	key ^= key >> 31
	return key
}

const ncmps = 32

var cmplocks [ncmps]sync.Mutex
var cmptypes [ncmps]map[uint64]any

// compareFor returns compare and search functions for a type.
// intended to be used with the Table collection type
func compareFor[T any]() ctype[T] {
	// load from cache. Uses the interface type + mix13 hash
	id := mix13(uint64(gettypeid[T]()))
	cmpi := (id >> 24) % ncmps
	cmplocks[cmpi].Lock()
	x, ok := cmptypes[cmpi][id]
	cmplocks[cmpi].Unlock()
	if ok {
		return *x.(*ctype[T])
	}
	// process the type / fields.
	var c ctype[T]
	t := reflect.TypeFor[T]()
	var idr bool
	var bsearch bool
	for t.Kind() == reflect.Pointer {
		idr = true
		bsearch = true
		t = t.Elem()
	}
	if t.Kind() == reflect.Struct {
		compareForStruct(&c, t, bsearch, idr)
	} else if !idr {
		compareForType(&c, t, bsearch)
	}
	if c.compare != nil {
		c.idr = idr
		c.ok = true
	}
	// save to cache
	c2 := c
	c3 := &c2
	cmplocks[cmpi].Lock()
	if cmptypes[cmpi] == nil {
		cmptypes[cmpi] = make(map[uint64]any)
	}
	cmptypes[cmpi][id] = c3
	cmplocks[cmpi].Unlock()
	return c
}

// Return a compare function for type or nil if type is not comparable
func CompareFor[T any]() func(T, T) int {
	c := compareFor[T]()
	if c.compare == nil {
		return nil
	}
	return func(a, b T) int {
		return c.compare(omit{}, a, omit{}, b)
	}
}

////////////////////////////////////////////////////////////////////////////////
// Deque
////////////////////////////////////////////////////////////////////////////////

// Deque is a double-ended queue
type Deque[T any] struct {
	tree tree[omit, T]
}

type DequeOptions[T any] struct {
	Copy    func(T) T
	Release func(T)
}

func NewDequeOptions[T any](opts DequeOptions[T]) *Deque[T] {
	b := new(Deque[T])
	b.tree.dataCopy = opts.Copy
	b.tree.dataRelease = opts.Release
	return b
}

func NewDeque[T any]() *Deque[T] {
	return NewDequeOptions(DequeOptions[T]{})
}

func (b *Deque[T]) PushFront(item T) {
	b.tree.PushFront(omit{}, item)
}

func (b *Deque[T]) PushBack(item T) {
	b.tree.PushBack(omit{}, item)
}

func (b *Deque[T]) PopFront() (T, bool) {
	_, val, ok := b.tree.PopFront()
	return val, ok
}

func (b *Deque[T]) PopBack() (T, bool) {
	_, val, ok := b.tree.PopBack()
	return val, ok
}

func (b *Deque[T]) PopFrontIf(cond func(item T) bool) (T, bool) {
	if cond == nil {
		return b.PopFront()
	}
	_, value, ok := b.tree.PopFrontIf(func(_ omit, item T) bool {
		return cond(item)
	})
	return value, ok
}

func (b *Deque[T]) PopBackIf(cond func(item T) bool) (T, bool) {
	if cond == nil {
		return b.PopBack()
	}
	_, value, ok := b.tree.PopBackIf(func(_ omit, item T) bool {
		return cond(item)
	})
	return value, ok
}

func (b *Deque[T]) Front() (T, bool) {
	_, item, ok := b.tree.Front()
	return item, ok
}

func (b *Deque[T]) Back() (T, bool) {
	_, item, ok := b.tree.Back()
	return item, ok
}

func (b *Deque[T]) FrontMut() (T, bool) {
	_, item, ok := b.tree.FrontMut()
	return item, ok
}

func (b *Deque[T]) BackMut() (T, bool) {
	_, item, ok := b.tree.BackMut()
	return item, ok
}

// At returns the item At position, after first, in queue.
// Returns false if no item is found At position.
func (b *Deque[T]) At(pos int) (T, bool) {
	_, item, ok := b.tree.GetAt(pos)
	return item, ok
}

// AtMut returns the item at position, after first, in queue.
// Returns false if no item is found at position.
// MUTABLE OPERATION.
func (b *Deque[T]) AtMut(pos int) (T, bool) {
	_, item, ok := b.tree.GetAtMut(pos)
	return item, ok
}

// Len returns the number of items in queue
func (b *Deque[T]) Len() int {
	return b.tree.Len()
}

// Copy the queue.
// This is a fast O(1) operation using a copy-on-write method.
func (b *Deque[T]) Copy() *Deque[T] {
	b2 := new(Deque[T])
	*b2 = *b
	b.tree.CopyInto(&b2.tree)
	return b2
}

// Clear the queue.
func (b *Deque[T]) Clear() {
	b.tree.Clear()
}

// Release will clear the queue and release any references.
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

// All returns an iterator of all items starting with the last.
func (b *Deque[T]) Backward() iter.Seq[T] {
	return iterV(b.tree.Backward())
}

// All returns an iterator of all items starting with the last.
// MUTABLE OPERATION.
func (b *Deque[T]) BackwardMut() iter.Seq[T] {
	return iterV(b.tree.BackwardMut())
}

func (b *Deque[T]) Drain() iter.Seq[T] {
	return iterV(b.tree.Drain())
}

func (b *Deque[T]) DrainBackward() iter.Seq[T] {
	return iterV(b.tree.DrainBackward())
}

////////////////////////////////////////////////////////////////////////////////
// Prique
////////////////////////////////////////////////////////////////////////////////

type pqitem[T any] struct {
	value T      `btree:"key"`
	seqid uint64 `btree:"key"`
}

type Prique[T any] struct {
	table Table[pqitem[T]]
	seqid uint64
}

type PriqueOptions[T any] struct {
	Less    func(T, T) bool
	Compare func(T, T) int
	Copy    func(T) T
	Release func(T)
}

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

func NewPrique[T any]() *Prique[T] {
	return NewPriqueOptions(PriqueOptions[T]{})
}

func (b *Prique[T]) Push(item T) bool {
	b.seqid++
	_, ok := b.table.Insert(pqitem[T]{item, b.seqid})
	return ok
}

func (b *Prique[T]) All() iter.Seq[T] {
	return func(yield func(T) bool) {
		for v := range b.table.Backward() {
			if !yield(v.value) {
				break
			}
		}
	}
}

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

// Pop first item from queue.
func (b *Prique[T]) Pop() (T, bool) {
	v, ok := b.table.PopBack()
	return v.value, ok
}

func (b *Prique[T]) PopIf(cond func(item T) bool) (T, bool) {
	if cond == nil {
		return b.Pop()
	}
	v, ok := b.table.PopBackIf(func(item pqitem[T]) bool {
		return cond(item.value)
	})
	return v.value, ok
}

func (b *Prique[T]) Front() (T, bool) {
	v, ok := b.table.Back()
	return v.value, ok
}

func (b *Prique[T]) FrontMut() (T, bool) {
	v, ok := b.table.BackMut()
	return v.value, ok
}

// At returns the item At position, after first, in queue.
// Returns false if no item is found At position.
func (b *Prique[T]) At(pos int) (T, bool) {
	v, ok := b.table.GetAt(b.table.Len() - pos - 1)
	return v.value, ok
}

// AtMut returns the item at position, after first, in queue.
// Returns false if no item is found at position.
// MUTABLE OPERATION.
func (b *Prique[T]) AtMut(pos int) (T, bool) {
	v, ok := b.table.GetAtMut(b.table.Len() - pos - 1)
	return v.value, ok
}

// Len returns the number of items in queue
func (b *Prique[T]) Len() int {
	return b.table.Len()
}

// Copy the queue.
// This is a fast O(1) operation using a copy-on-write method.
func (b *Prique[T]) Copy() *Prique[T] {
	b2 := new(Prique[T])
	*b2 = *b
	b2.table = *b.table.Copy()
	return b2
}

// Clear the queue.
func (b *Prique[T]) Clear() {
	b.table.Clear()
	b.seqid = 0
}

// Release will clear the queue and release any references.
// This method is functionally equivalent to Clear() but is an optimization for
// collections that are copied using Copy().
func (b *Prique[T]) Release() {
	b.table.Release()
	b.seqid = 0
}
