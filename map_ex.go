package btree

func (b *Map[K, V]) nodeInsertNS(n *node[K, V], key K, value V) (V, int) {
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
		return b.nodeInsertNS(n, key, value)
	}
	child = n.branch.children[i]
	if !child.leaf() {
		prev, inserted := b.nodeInsertNS(child, key, value)
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

func (b *Map[K, V]) insertNS(key K, value V) (V, bool) {
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
		current, inserted := b.nodeInsertNS(b.tree.root, key, value)
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

func (b *Map[K, V]) strSearch(n *node[K, V], prefix uint64, key K,
) (i int, found bool) {
	if n.leaf() {
		// bsearch
		if key > n.keys[n.len-1] {
			return n.len, false
		}
		low, high := 0, n.len
		for low < high {
			h := (low + high) >> 1
			if !(key < n.keys[h]) {
				low = h + 1
			} else {
				high = h
			}
		}
		if low > 0 && !(n.keys[low-1] < key) {
			return low - 1, true
		}
		return low, false
	} else {
		if prefix > n.branch.prefixes[n.len-1] {
			return n.len, false
		}
		for prefix > n.branch.prefixes[i] {
			i++
		}
		if prefix < n.branch.prefixes[i] {
			return i, false
		}
		for ; i < n.len; i++ {
			if key <= n.keys[i] {
				return i, key == n.keys[i]
			}
		}
		return i, false
	}
}

func (b *Map[K, V]) strNodeInsert(n *node[K, V], prefix uint64, key K,
	value V,
) (V, int) {
	var emptyValue V
	i, found := b.strSearch(n, prefix, key)
	if found {
		return n.values[i], 0
	}
	if n.leaf() {
		n.insertItemAt(key, value, i)
		return emptyValue, 1
	}
	b.tree.cowChild(n, i, true)
	if b.tree.ensureBranch(n, i) {
		return b.strNodeInsert(n, prefix, key, value)
	}
	prev, inserted := b.strNodeInsert(n.branch.children[i], prefix, key, value)
	n.branch.counts[i] += inserted
	return prev, inserted
}

func (b *Map[K, V]) strInsert(key K, value V) (V, bool) {
	prefix := prefixString(key)
	var emptyValue V
	if b.tree.count == 0 {
		b.tree.insertFirstItem(key, value)
		return emptyValue, true
	}
	b.tree.cowRoot(true)
	b.tree.ensureRoot()
	current, inserted := b.strNodeInsert(b.tree.root, prefix, key, value)
	b.tree.count += inserted
	return current, inserted == 1
}

func (b *Map[K, V]) insertAlt(key K, value V) (V, bool) {
	if b.tree.strprefix {
		return b.strInsert(key, value)
	} else {
		return b.tree.Insert(key, value)
	}
}

func (b *Map[K, V]) strReplace(key K, value V) (V, bool) {
	var emptyValue V
	if b.tree.count == 0 {
		return emptyValue, false
	}
	prefix := prefixString(key)
	b.tree.cowRoot(true)
	n := b.tree.root
	for {
		i, found := b.strSearch(n, prefix, key)
		if found {
			old := n.values[i]
			n.values[i] = value
			return old, true
		}
		if n.leaf() {
			return emptyValue, false
		}
		b.tree.cowChild(n, i, true)
		n = n.branch.children[i]
	}
}

func (b *Map[K, V]) altReplace(key K, value V) (V, bool) {
	if b.tree.strprefix {
		return b.strReplace(key, value)
	} else {
		return b.tree.Replace(key, value)
	}
}

func (b *Map[K, V]) replaceNS(key K, value V) (V, bool) {
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

func (b *Map[K, V]) replaceAlt(key K, value V) (V, bool) {
	if b.tree.strprefix {
		return b.strReplace(key, value)
	} else {
		return b.tree.Replace(key, value)
	}
}

func (b *Map[K, V]) nodeSetNS(n *node[K, V], key K, value V) (V, int) {
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
		return b.nodeSetNS(n, key, value)
	}
	child = n.branch.children[i]
	if !child.leaf() {
		prev, inserted := b.nodeSetNS(child, key, value)
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

func (b *Map[K, V]) setNS(key K, value V) (V, bool) {
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
		current, inserted := b.nodeSetNS(b.tree.root, key, value)
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

func (b *Map[K, V]) strNodeSet(n *node[K, V], prefix uint64, key K, value V,
) (V, int) {
	var emptyValue V
	i, found := b.strSearch(n, prefix, key)
	if found {
		old := n.values[i]
		n.values[i] = value
		return old, 0
	}
	if n.leaf() {
		n.insertItemAt(key, value, i)
		return emptyValue, 1
	}
	b.tree.cowChild(n, i, true)
	if b.tree.ensureBranch(n, i) {
		return b.strNodeSet(n, prefix, key, value)
	}
	prev, inserted := b.strNodeSet(n.branch.children[i], prefix, key, value)
	n.branch.counts[i] += inserted
	return prev, inserted
}

func (b *Map[K, V]) strSet(key K, value V) (V, bool) {
	prefix := prefixString(key)
	var emptyValue V
	if b.tree.count == 0 {
		b.tree.insertFirstItem(key, value)
		return emptyValue, false
	}
	b.tree.cowRoot(true)
	b.tree.ensureRoot()
	current, inserted := b.strNodeSet(b.tree.root, prefix, key, value)
	b.tree.count += inserted
	return current, inserted == 0
}

func (b *Map[K, V]) setAlt(key K, value V) (V, bool) {
	if b.tree.strprefix {
		return b.strSet(key, value)
	} else {
		return b.tree.Set(key, value)
	}
}

func (b *Map[K, V]) nodeDeleteNS(n *node[K, V], key K) (V, bool) {
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
		old, deleted = b.nodeDeleteNS(n.branch.children[i], key)
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

func (b *Map[K, V]) deleteNS(key K) (V, bool) {
	var emptyValue V
	if b.tree.count == 0 {
		return emptyValue, false
	}
	b.tree.cowRoot(true)
	old, deleted := b.nodeDeleteNS(b.tree.root, key)
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

func (b *Map[K, V]) strNodeDelete(n *node[K, V], prefix uint64, key K,
) (V, bool) {
	var emptyKey K
	var emptyValue V
	i, found := b.strSearch(n, prefix, key)
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
		if n.branch.prefixes != nil {
			n.branch.prefixes[i] = prefixString(maxKey)
		}
	} else {
		old, deleted = b.strNodeDelete(n.branch.children[i], prefix, key)
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

func (b *Map[K, V]) strDelete(key K) (V, bool) {
	var emptyValue V
	if b.tree.count == 0 {
		return emptyValue, false
	}
	prefix := prefixString(key)
	b.tree.cowRoot(true)
	old, deleted := b.strNodeDelete(b.tree.root, prefix, key)
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

func (b *Map[K, V]) deleteAlt(key K) (V, bool) {
	if b.tree.strprefix {
		return b.strDelete(key)
	} else {
		var emptyValue V
		return b.tree.Delete(key, emptyValue)
	}
}

func (b *Map[K, V]) lookupNS(key K) (V, bool) {
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

func (b *Map[K, V]) strLookup(key K, mut bool) (V, bool) {
	var emptyValue V
	if b.tree.count == 0 {
		return emptyValue, false
	}
	prefix := prefixString(key)
	b.tree.cowRoot(mut)
	n := b.tree.root
	for {
		i, found := b.strSearch(n, prefix, key)
		if found {
			return n.values[i], true
		}
		if n.leaf() {
			return emptyValue, false
		}
		b.tree.cowChild(n, i, mut)
		n = n.branch.children[i]
	}
}

func (b *Map[K, V]) lookupAlt(key K) (V, bool) {
	if b.tree.strprefix {
		return b.strLookup(key, false)
	} else {
		var emptyValue V
		return b.tree.Lookup(key, emptyValue)
	}
}

func (b *Map[K, V]) lookupNSMut(key K) (V, bool) {
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

func (b *Map[K, V]) lookupAltMut(key K) (V, bool) {
	if b.tree.strprefix {
		return b.strLookup(key, true)
	} else {
		var emptyValue V
		return b.tree.LookupMut(key, emptyValue)
	}
}
