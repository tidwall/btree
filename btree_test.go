package btree

import (
	"bytes"
	"cmp"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// Tiny C-like assert. For a robust assert: github.com/tidwall/assert.
//
//go:noinline
func assert(cond bool) {
	if !cond {
		_, path, ln, _ := runtime.Caller(1)
		data, _ := os.ReadFile(path)
		fmt.Fprintf(os.Stderr, "Assertion failed: %s, file %s, line %d.\n",
			"("+strings.SplitN(strings.TrimSpace(strings.Split(string(data),
				"\n")[ln-1]), "(", 2)[1], filepath.Base(path), ln)
		os.Exit(6)
	}
}

func (t *tree[K, V]) saneItemIsEmpty(n *node[K, V], i int,
	itemIsEmpty func(K, V) bool,
) bool {
	if itemIsEmpty != nil && !itemIsEmpty(n.keys[i], n.values[i]) {
		return false
	}
	return true
}

func (t *tree[K, V]) saneDeepCount(n *node[K, V]) int {
	count := n.len
	if !n.leaf() {
		for i := 0; i <= n.len; i++ {
			count += t.saneDeepCount(n.branch.children[i])
		}
	}
	return count
}

func (t *tree[K, V]) saneCompare(an *node[K, V], ai int, bn *node[K, V],
	bi int,
) int {
	return t.dataCompare(an.keys[ai], an.values[ai], bn.keys[bi], bn.values[bi])
}

func (t *tree[K, V]) saneNode(n *node[K, V], depth, height int,
	isempty func(K, V) bool,
) int {
	assert(unsafe.Pointer(n) == unsafe.Pointer(&n.keys[0]))
	count := n.len
	if n.len < minItems {
		assert(depth <= 0)
	}
	assert(n.len <= maxItems)
	if t.dataCompare != nil {
		for i := 1; i < n.len; i++ {
			assert(t.saneCompare(n, i-1, n, i) < 0)
		}
	}
	for i := n.len; i < maxItems; i++ {
		assert(t.saneItemIsEmpty(n, i, isempty))
	}
	if n.leaf() {
		assert(depth == height)
		return count
	}
	if t.dataCompare != nil {
		for i := 0; i < n.len; i++ {
			left := n.branch.children[i]
			right := n.branch.children[i+1]
			assert(t.saneCompare(left, left.len-1, n, i) < 0)
			assert(t.saneCompare(n, i, right, 0) < 0)
		}
	}
	for i := 0; i <= n.len; i++ {
		c := t.saneDeepCount(n.branch.children[i])
		assert(n.branch.counts[i] == c)
	}
	for i := 0; i <= n.len; i++ {
		count += t.saneNode(n.branch.children[i], depth+1, height, isempty)
	}
	for i := n.len + 1; i <= maxItems; i++ {
		assert(n.branch.children[i] == nil)
	}
	return count
}

func (t *tree[K, V]) Sane(isempty func(K, V) bool) {
	height := t.Height()
	var count int
	if t.root != nil {
		count = t.saneNode(t.root, 0, height, isempty)
	}
	assert(count == t.count)
}

func TestMapBasic1(t *testing.T) {
	var m Map[int, int]
	itemIsEmpty := func(k, v int) bool {
		return k == 0 && v == 0
	}
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))
	for i, j := range rng.Perm(100000) {
		key := j * 10

		val, found := m.Get(key)
		assert(!found && val == 0)

		val, found = m.GetMut(key)
		assert(!found && val == 0)

		assert(!m.Contains(key))

		old, deleted := m.Delete(key)
		assert(!deleted && old == 0)

		old, replaced := m.Set(key, -key)
		assert(!replaced && old == 0)

		old, deleted = m.Delete(key)
		assert(deleted && old == -key)

		old, replaced = m.Replace(key, key)
		assert(!replaced && old == 0)

		existing, inserted := m.Insert(key, -key)
		assert(inserted && existing == 0)

		existing, inserted = m.Insert(key, -key)
		assert(!inserted && existing == -key)

		old, replaced = m.Replace(key, key)
		assert(replaced && old == -key)

		old, replaced = m.Replace(key, -key)
		assert(replaced && old == key)

		val, found = m.Get(key)
		assert(found && val == -key)

		assert(m.Contains(key))

		key2, val, found := m.Seek(key)
		assert(found && key2 == key && val == -key)

		key2, val, found = m.Seek(key - 5)
		assert(found && key2 == key && val == -key)

		key2, val, found = m.SeekNext(key)
		if !found {
			key3, val, found := m.Back()
			assert(found && key3 == key && val == -key)
		} else {
			assert(key2 > key && val == -key2)
		}

		if i%131 == 0 {
			m.tree.Sane(itemIsEmpty)
		}
	}
	m.tree.Sane(itemIsEmpty)
}

func TestMapBasic2(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))
	N := 10000
	keys := rng.Perm(N)
	var c Map[int, int]
	assert(len(slices.Collect(c.Keys())) == 0)
	assert(len(slices.Collect(c.Values())) == 0)
	for i := range N {
		if c.Len() != i {
			t.Fatalf("expected %d got %d", i, c.Len())
		}

		val, found := c.Get(keys[i])
		assert(!found && val == 0)

		val, inserted := c.Insert(keys[i], keys[i])
		assert(inserted && val == 0)
		assert(c.Len() == i+1)
		c.tree.Sane(nil)

		val, found = c.Get(keys[i])
		assert(found && val == keys[i])

		val, deleted := c.Delete(keys[i])
		assert(deleted && val == keys[i])

		val, deleted = c.Delete(keys[i])
		assert(!deleted && val == 0)

		val, inserted = c.Insert(keys[i], keys[i])
		assert(inserted && val == 0)
		assert(c.Len() == i+1)
		c.tree.Sane(nil)

		val, deleted = c.Delete(keys[i])
		assert(deleted && val == keys[i])

		val, deleted = c.Delete(keys[i])
		assert(!deleted && val == 0)

		val, inserted = c.Insert(keys[i], keys[i])
		assert(inserted && val == 0)
		assert(c.Len() == i+1)
		c.tree.Sane(nil)

		j, found := c.IndexOf(keys[i])
		assert(found)

		key, val, found := c.GetAt(j)
		assert(found && key == keys[i] && val == keys[i])
	}
	var key int
	var val int
	var found bool
	key, val, found = c.GetAt(-1)
	assert(!found && key == 0 && val == 0)

	key, val, found = c.GetAt(N)
	assert(!found && key == 0 && val == 0)

	for i := range N {
		key, val, found := c.GetAt(i)
		assert(found && key == i && val == i)
	}
	// shuffle
	keys = rng.Perm(N)

	for i := range N {
		val, found := c.Get(keys[i])
		assert(found && val == keys[i])

		val, inserted := c.Insert(keys[i], -keys[i])
		assert(!inserted && val == keys[i])

		val, replaced := c.Replace(keys[i], -keys[i])
		assert(replaced && val == keys[i])

		val, found = c.Get(keys[i])
		assert(found && val == -keys[i])
		c.tree.Sane(nil)
	}

	for i := range N {
		key, val, found := c.GetAt(i)
		assert(found && key == i && val == -i)
	}

	// shuffle
	keys = rng.Perm(N)

	for i := range N {
		val, found := c.Get(keys[i])
		assert(found && val == -keys[i])

		val, deleted := c.Delete(keys[i])
		assert(deleted && val == -keys[i])

		val, found = c.Get(keys[i])
		assert(!found && val == 0)

		val, deleted = c.Delete(keys[i])
		assert(!deleted && val == 0)
		c.tree.Sane(nil)
	}

	// shuffle
	keys = rng.Perm(N)

	for i := range N {
		val, inserted := c.Insert(keys[i], -keys[i])
		assert(inserted && val == 0)
	}
	k, v, replaced := c.ReplaceAt(-1, 0, 0)
	assert(!replaced && k == 0 && v == 0)

	k, v, replaced = c.ReplaceAt(N+1, 0, 0)
	assert(!replaced && k == 0 && v == 0)

	for i := range N {
		key2 := ((N - i) * 10)
		key, val, replaced := c.ReplaceAt(i, -key2, key2)
		assert(replaced && key == i && val == -key)
	}
	for i := range N {
		key2 := ((N - i) * 10)
		if i < N-1 {
			_, _, replaced := c.ReplaceAt(i, (-key2)+1000, key2)
			assert(!replaced)
		}
		if i > 0 {
			_, _, replaced := c.ReplaceAt(i, (-key2)-1000, key2)
			assert(!replaced)
		}
	}
	k, v, deleted := c.DeleteAt(-1)
	assert(!deleted && k == 0 && v == 0)

	for i := range N {
		key, val, deleted := c.DeleteAt(i)
		assert(deleted && val == -key)

		val, inserted := c.Insert(i, -i)
		assert(inserted && val == 0)
	}

	c.Clear()
	assert(c.Len() == 0)
}

func TestMapPushPop(t *testing.T) {
	N := 10000
	var c Map[int, int]
	isempty := func(k, v int) bool {
		return k == 0 && v == 0
	}

	key, value, deleted := c.PopFront()
	assert(!deleted && key == 0 && value == 0)

	key, value, ok := c.Front()
	assert(!ok && key == 0 && value == 0)

	key, value, deleted = c.PopBack()
	assert(!deleted && key == 0 && value == 0)

	key, value, ok = c.Back()
	assert(!ok && key == 0 && value == 0)

	for i := range N {
		key := N - i - 1
		ok := c.PushFront(key, -key)
		assert(ok)
		c.tree.Sane(isempty)

		ok = c.PushFront(key, -key)
		assert(!ok)
		assert(c.Len() == i+1)

		k, v, ok := c.Front()
		assert(ok && k == key && v == -k)

		k, v, ok = c.Back()
		assert(ok && k == N-1 && v == -k)
	}
	c.tree.Sane(isempty)
	for i := range N {
		key, value, deleted := c.PopFront()
		assert(deleted && key == i && value == -key)
		assert(c.Len() == N-i-1)
		c.tree.Sane(isempty)
	}
	c.tree.Sane(isempty)
	assert(c.Len() == 0)

	for i := range N {
		key := i

		ok := c.PushBack(key, -key)
		assert(ok)
		c.tree.Sane(isempty)

		ok = c.PushBack(key, -key)
		assert(!ok)
	}
	c.tree.Sane(isempty)
	for i := range N {
		key, value, ok := c.PopBack()
		assert(ok && key == N-i-1 && value == -key)
		c.tree.Sane(isempty)
	}
}

func TestMapIter(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var m Map[int, int]
	N := 100000
	for _, key := range rng.Perm(N) {
		old, inserted := m.Insert(key, -key)
		assert(inserted && old == 0)
	}
	m.tree.Sane(nil)
	for range 100 {
		n := rng.IntN(N)
		var i int
		for key, value := range m.All() {
			assert(key == i && value == -key)
			i++
			if n == i {
				break
			}
		}
	}
	for range 100 {
		n := rng.IntN(N)
		var i int
		for key, value := range m.Backward() {
			assert(key == N-i-1 && value == -key)
			i++
			if n == i {
				break
			}
		}
	}
	for j := 0; j < N; j += 1313 {
		for range 200 {
			n := rng.IntN(N)
			var i int
			for key, value := range m.Ascend(j) {
				assert(key == j+i && value == -key)
				i++
				if n == i {
					break
				}
			}
		}
	}

	for j := 0; j < N; j += 1313 {
		for range 200 {
			n := rng.IntN(N)
			var i int
			for key, value := range m.Descend(N - j - 1) {
				assert(key == (N-i-1)-j && value == -key)
				i++
				if n == i {
					break
				}
			}
		}
	}
}

func shuffle[T any](rng *rand.Rand, slice []T) {
	for i := range slice {
		j := rng.IntN(i + 1)
		slice[i], slice[j] = slice[j], slice[i]
	}
}

func testMapDeleteRange(rng *rand.Rand, N int) time.Duration {

	var arr []int

	// fmt.Printf("============ DELETERANGE (%d items) ============\n", N)

	var m Map[int, int]
	vempty := func(k, v int) bool {
		return k == 0 && v == 0
	}
	for _, key := range rng.Perm(N) {

		m.Insert(key, -key)
		arr = append(arr, key)
	}
	// c.Print()
	m.tree.Sane(vempty)
	slices.Sort(arr)
	var keys []int
	for key := range m.All() {
		keys = append(keys, key)
	}

	assert(slices.Equal(keys, arr))
	s := (rng.Int() % N) - 1
	e := (rng.Int() % N) + 1
	if s > e {
		s, e = e, s
	}

	var minexcl bool
	if s == -1 {
		if N > 1 {
			minexcl = true
			s = 1
		} else {
			s = 0
		}
	}
	_ = minexcl

	arr = slices.Delete(arr, s, e)

	start := time.Now()
	var opts *DeleteRangeOptions
	var keyMin int
	if minexcl {
		keyMin = keys[s-1]
		opts = &DeleteRangeOptions{}
		opts.MinExclusive = true
	} else {
		keyMin = keys[s]
	}

	var keyMax int
	if e == N {
		keyMax = keys[e-1]
		if opts == nil {
			opts = &DeleteRangeOptions{}
		}
		opts.MaxInclusive = true
	} else {
		keyMax = keys[e]
	}

	var list Slice2[int, int]
	if opts != nil {
		list = m.DeleteRangeOptions(keyMin, keyMax, *opts)
	} else {
		list = m.DeleteRange(keyMin, keyMax)
	}
	elapsed := time.Since(start)
	assert(len(arr) == m.Len())
	m.tree.Sane(vempty)
	// return elapsed

	var last int
	var extracted []int
	var listlen int
	for key, value := range list.All() {
		listlen++
		assert(value == -key)
		assert(len(extracted) <= 0 || key > last)
		extracted = append(extracted, key)
		last = key
	}
	assert(listlen == N-m.Len())
	assert(list.Len() == listlen)
	assert(len(extracted) == N-m.Len())

	var extracted2 []int
	for key, value := range list.All() {
		assert(value == -key)
		assert(len(extracted2) <= 0 || key > last)
		extracted2 = append(extracted2, key)
		last = key
	}

	var extracted3 []int
	for key, value := range list.Backward() {
		assert(value == -key)
		extracted3 = append(extracted3, key)
	}

	assert(len(extracted) == len(extracted2))
	assert(slices.IsSorted(extracted))
	assert(slices.IsSorted(extracted2))
	assert(len(extracted3) == len(extracted2))
	assert(slices.IsSortedFunc(extracted3, func(a, b int) int {
		return cmp.Compare(b, a)
	}))
	for i := range extracted3 {
		assert(extracted3[i] == extracted[len(extracted)-1-i])
	}

	for _, key := range extracted {
		_, ok := m.Get(key)
		assert(!ok)
	}

	// perform 1 step check of all iterator
	n := min(list.Len(), 2000)
	for i := range n {
		var j int
		for key, value := range list.All() {
			if j == i {
				break
			}
			assert(value == -key)
			assert(key == extracted2[j])
			j++
		}
	}

	// perform 1 step check of backward iterator
	for i := range n {
		var j int
		for key, value := range list.Backward() {
			if j == i {
				break
			}
			assert(value == -key)
			assert(key == extracted3[j])
			j++
		}
	}

	return elapsed
}

func TestMapDeleteRange(t *testing.T) {
	// t.Skip()
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	var totalElapsed time.Duration
	totalN := 0
	rng := rand.New(rand.NewPCG(0, seed))
	start := time.Now()
	for time.Since(start) < time.Second*5 {
		var N int
		switch rng.Int() % 4 {
		case 0:
			N = rng.Int() % 10
		case 1:
			N = rng.Int() % 100
		case 2:
			N = rng.Int() % 1000
		case 3:
			N = rng.Int() % 10000
			// case 4:
			// 	N = rng.Int() % 100000
			// case 5:
			// 	N = rng.Int() % 1000000
		}
		if N == 0 {
			continue
		}
		totalN += N
		totalElapsed += testMapDeleteRange(rng, N)
	}
	// print("delrange ")
	// lotsa.WriteOutput(os.Stdout, totalN, 1, totalElapsed, 0)

	var m Map[int, int]
	for n := 1; n < 100; n++ {
		for i := 0; i < n; i++ {
			m.Set(i, i)
		}
		list := m.DeleteRangeOptions(n-1, n+1,
			DeleteRangeOptions{MinExclusive: true})
		assert(m.Len() == n && list.Len() == 0)
	}
}

func TestMapDeleteRangeAtBounds(t *testing.T) {
	var m Map[int, int]
	var opts DeleteRangeOptions
	list := m.DeleteRangeAtOptions(-1, 0, opts)
	assert(list.Len() == 0)
	list = m.DeleteRangeAtOptions(-1, 0, opts)
	assert(list.Len() == 0)
	list = m.DeleteRangeAtOptions(0, -1, opts)
	assert(list.Len() == 0)
	list = m.DeleteRangeAtOptions(1, 1, opts)
	assert(list.Len() == 0)

	// // micro unit
	// m.Insert(1, 1)
	// list2, dr := m.deleteRangeAt1(0, 1, false)
	// assert(list2.Len() == 1)
	// assert(dr.start == 0 && dr.count == 1)

	// m.Insert(1, 1)
	// assert(m.Len() == 1)
	// list = m.DeleteRangeAt(0, 1, DeleteRangeOptions{NoReturn: true})
	// assert(m.Len() == 0)

}

func TestMapKeysValues(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))
	type item struct {
		key   int
		value int
	}
	var c Map[int, int]
	var items []item
	for _, key := range rng.Perm(100000) {
		items = append(items, item{key, -key})
		c.Insert(key, -key)
	}
	slices.SortFunc(items, func(a, b item) int {
		return cmp.Compare(a.key, b.key)
	})

	var keys []int
	var values []int
	for _, item := range items {
		keys = append(keys, item.key)
		values = append(values, item.value)
	}
	assert(slices.Equal(keys, slices.Collect(c.Keys())))
	assert(slices.Equal(values, slices.Collect(c.Values())))
}

func TestMapCOW(t *testing.T) {
	var vseed atomic.Uint64
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(vseed.Add(1), seed))
	N := 1000
	const maxDepth = 3

	type utype struct {
		value string
	}

	vempty := func(k int, v *utype) bool {
		return k == 0 && v == nil
	}

	c := NewMapOptions(MapOptions[int, *utype]{
		Copy: func(v *utype) *utype {
			return &utype{value: v.value + ":"}
		},
	})

	for range N {
		key := rng.Int()
		val, inserted := c.Insert(key, &utype{strconv.Itoa(key)})
		assert(inserted && val == nil)
	}

	var wg sync.WaitGroup
	var copyAndDeepTest func(c *Map[int, *utype], depth int)
	var deepTest func(c *Map[int, *utype], depth int)

	variousStuff := func(c *Map[int, *utype]) {
		// Do various stuff with the collection. Basically make a bunch of
		// changes and make sure everything is sane
		rng := rand.New(rand.NewPCG(vseed.Add(1), seed))
		n := c.Len()
		keys := slices.Collect(c.Keys())
		vals := slices.Collect(c.ValuesMut())

		var i int
		for key, value := range c.AllMut() {
			assert(key == keys[i])
			assert(value == vals[i])
			i++
		}

		for key, value := range c.BackwardMut() {
			i--
			assert(key == keys[i])
			assert(value == vals[i])
		}

		for i := range n {
			key, val, found := c.GetAtMut(i)
			assert(found)
			if rng.Int()%2 == 0 {
				val2, deleted := c.Delete(key)
				assert(deleted && val2.value == val.value)

				val3, inserted := c.Insert(key, val)
				assert(inserted && val3 == nil)
			}
			key2, val2, found := c.GetAt(i)
			assert(found && key2 == key && val2.value == val.value)

			val3, found := c.GetMut(key2)
			assert(found && val3.value == val2.value)

			key4, val4, found := c.FrontMut()
			assert(found && key4 == keys[0] && val4.value == vals[0].value)

			key5, val5, found := c.BackMut()
			assert(found)
			assert(key5 == keys[len(keys)-1])
			assert(val5.value == vals[len(vals)-1].value)

		}
		val, found := c.GetMut(-1)
		assert(!found && val == nil)

		c.tree.Sane(vempty)

		keys2 := slices.Collect(c.Keys())
		vals2 := slices.Collect(c.Values())
		assert(slices.Equal(keys, keys2))
		equals := slices.EqualFunc(vals, vals2, func(v1, v2 *utype) bool {
			if v1.value != v2.value {
				println(v1.value, v2.value)
			}
			return v1.value == v2.value
		})
		assert(equals)

		if n > 5 {
			k0, v0, ok := c.GetAt(n / 3)
			assert(ok)
			assert(v0.value == vals2[n/3].value)
			k1, v1, ok := c.GetAt(n - n/3)
			assert(v1.value == vals2[n-n/3].value)

			list := c.DeleteRange(k0, k1)
			var i int
			for k, v := range list.All() {
				assert(k == keys2[n/3+i])
				assert(v.value == vals2[n/3+i].value)
				i++
			}
		}
	}

	deepTest = func(c *Map[int, *utype], depth int) {
		defer wg.Done()
		for range 5 {
			variousStuff(c)
			copyAndDeepTest(c, depth+1)
		}
		variousStuff(c)
	}

	copyAndDeepTest = func(c *Map[int, *utype], depth int) {
		if depth > maxDepth {
			return
		}
		c2 := c.Copy()
		wg.Add(2)
		go deepTest(c2, depth)
		deepTest(c, depth)
	}

	copyAndDeepTest(c, 0)

	wg.Wait()
}

func TestMapCOWNoValueCopy(t *testing.T) {
	var m Map[int, int]
	N := 10000
	for i := range N {
		val, replaced := m.Set(i, i)
		assert(val == 0 && replaced == false)
	}
	m.tree.Sane(nil)
	m2 := m.Copy()
	assert(slices.Equal(slices.Collect(m.Keys()), slices.Collect(m2.Keys())))
	assert(slices.Equal(slices.Collect(m.Values()),
		slices.Collect(m2.Values())))

	for i := range N {
		val, replaced := m2.Set(i, -i)
		assert(val == i && replaced == true)
		val, found := m2.Get(i)
		assert(val == -i && found == true)
	}

	assert(slices.Equal(slices.Collect(m.Keys()), slices.Collect(m2.Keys())))
	assert(!slices.Equal(slices.Collect(m.Values()),
		slices.Collect(m2.Values())))

	m2.tree.Sane(nil)
}

func TestMapSeekIter(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))
	var m Map[int, int]
	N := 1000
	for i := range N {
		key := i * 10
		m.Set(key, i)
	}
	var j, M int
	for i := range N {
		key := i * 10
		k, v, ok := m.Seek(key)
		assert(ok && k == i*10 && v == i)
		k, v, ok = m.SeekMut(key)
		assert(ok && k == i*10 && v == i)
		if i > 0 {
			k, v, ok = m.SeekPrev(key)
			assert(ok && k == (i-1)*10 && v == i-1)
			k, v, ok = m.SeekPrevMut(key)
			assert(ok && k == (i-1)*10 && v == i-1)
		}
		if i < N-1 {
			k, v, ok = m.SeekNext(key)
			assert(ok && k == (i+1)*10 && v == i+1)
			k, v, ok = m.SeekNextMut(key)
			assert(ok && k == (i+1)*10 && v == i+1)
		}

		j = 0
		M = rng.Int()%100 + 4
		for key2, value2 := range m.Ascend(key) {
			assert(key2 == key+j*10 && value2 == key2/10)
			j++
			if j == M {
				break
			}
		}
		j = 0
		for key2, value2 := range m.AscendMut(key) {
			assert(key2 == key+j*10 && value2 == key2/10)
			j++
			if j == M {
				break
			}
		}

		j = 0
		for key2, value2 := range m.Descend(key) {
			assert(key2 == key+j*10 && value2 == key2/10)
			j--
			if j == -M {
				break
			}
		}
		j = 0
		for key2, value2 := range m.DescendMut(key) {
			assert(key2 == key+j*10 && value2 == key2/10)
			j--
			if j == -M {
				break
			}
		}
	}
}

func TestMapInsertAt(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))
	N := 1000

	_ = rng

	var m Map[int, int]

	i, ok := m.IndexOf(0)
	assert(i == 0 && !ok)

	for i := range N {
		key := i * 10
		m.Insert(key, i)
	}
	m.tree.Sane(nil)

	var j, M int
	for i := 0; i < N; i++ {
		j = 0
		M = rng.Int()%100 + 4
		for k, v := range m.AscendAt(i) {
			assert(k == (i+j)*10 && v == i+j)
			j++
			if j == M {
				break
			}
		}
		j = 0
		for k, v := range m.AscendAtMut(i) {
			assert(k == (i+j)*10 && v == i+j)
			j++
			if j == M {
				break
			}
		}
		j = 0
		for k, v := range m.DescendAt(i) {
			assert(k == (i+j)*10 && v == i+j)
			j--
			if j == -M {
				break
			}
		}
		j = 0
		for k, v := range m.DescendAtMut(i) {
			assert(k == (i+j)*10 && v == i+j)
			j--
			if j == -M {
				break
			}
		}
	}

	for i := N - 1; i >= 0; i-- {
		k, v, ok := m.GetAt(i)
		assert(ok && k == v*10)
		j, ok := m.IndexOf(k)
		assert(ok && i == j)
		assert(m.InsertAt(j, k-1, v))
		assert(!m.InsertAt(j, k, v))
		if i > 0 {
			assert(!m.InsertAt(j, k-1000000, v))
		}
		k2, v2, ok := m.GetAt(i)
		assert(ok && k2 == k-1 && v2 == v)

	}
	m.tree.Sane(nil)
	assert(m.Len() == N*2)

	assert(!m.InsertAt(-1, 1, 1))
	assert(!m.InsertAt(m.Len(), 0, -2))
	assert(m.InsertAt(m.Len(), 100000000, -2))

	k, v, ok := m.Back()
	assert(k == 100000000 && v == -2 && ok)

	keys := slices.Collect(m.Keys())
	values := slices.Collect(m.Values())

	var keys2 []int
	var values2 []int
	for m.Len() > 0 {
		k, v, ok := m.DeleteAt(0)
		assert(ok)
		keys2 = append(keys2, k)
		values2 = append(values2, v)
	}
	assert(slices.Equal(keys, keys2))
	assert(slices.Equal(values, values2))
}

func TestMapDeleteRangeAtIX(t *testing.T) {
	var m Map[int, int]
	for i := range 10000 {
		m.Set(i, i)
	}
	s := m.DeleteRangeAtOptions(3000, 1000, DeleteRangeOptions{
		MinExclusive: false,
		MaxInclusive: false,
	})
	assert(s.Len() == 1000)
	assert(m.Len() == 10000-s.Len())
	keys := slices.Collect(s.Keys())
	values := slices.Collect(s.Values())
	assert(len(keys) == s.Len())
	assert(len(values) == s.Len())
	for i := 0; i < len(keys); i++ {
		assert(keys[i] == i+3000)
		assert(values[i] == i+3000)
	}

	for i := range 10000 {
		m.Set(i, i)
	}
	s = m.DeleteRangeAtOptions(3000, 1000, DeleteRangeOptions{
		MinExclusive: true,
		MaxInclusive: false,
	})
	assert(s.Len() == 999)
	assert(m.Len() == 10000-s.Len())
	for i := range 10000 {
		m.Set(i, i)
	}
	s = m.DeleteRangeAtOptions(3000, 1000, DeleteRangeOptions{
		MinExclusive: true,
		MaxInclusive: true,
	})
	assert(s.Len() == 1000)
	assert(m.Len() == 10000-s.Len())
	for i := range 10000 {
		m.Set(i, i)
	}
	s = m.DeleteRangeAtOptions(3000, 1000, DeleteRangeOptions{
		MinExclusive: false,
		MaxInclusive: true,
	})
	assert(s.Len() == 1001)
	assert(m.Len() == 10000-s.Len())
	for i := range 10000 {
		m.Set(i, i)
	}

	s = m.DeleteRangeAtOptions(3000, 1000, DeleteRangeOptions{
		MinExclusive: false,
		MaxInclusive: true,
		NoReturn:     true,
	})
	assert(s.Len() == 0)
	assert(m.Len() == 10000-1001)

}

func TestMapCOWDataRelease(t *testing.T) {
	values := make(map[string]int)

	addValue := func(value string) {
		values[value] = values[value] + 1
	}
	delValue := func(value string) {
		values[value] = values[value] - 1
	}
	m := NewMapOptions(MapOptions[int, string]{
		Copy: func(value string) string {
			// value = value + ":"
			addValue(value)
			return value
		},
		Release: func(value string) {
			delValue(value)
		},
	})
	for i := range 10000 {
		key := i
		value := strconv.Itoa(i)
		addValue(value)
		m.Set(key, value)
	}
	assert(len(values) == m.Len())
	for _, n := range values {
		assert(n == 1)
	}
	nvalues := len(values)
	assert(nvalues == m.Len())
	m.Release()
	assert(m.Len() == 0)
	assert(len(values) == nvalues)
	for _, n := range values {
		assert(n == 0)
	}

	for i := range 10000 {
		key := i
		value := strconv.Itoa(key)
		addValue(value)
		m.Set(key, value)
	}

	m2 := m.Copy()
	for i := range 50 {
		key := i * 200
		value := strconv.Itoa(key)
		val, ok := m2.GetMut(key)
		assert(ok && val == value && values[value] == 2)
	}
	nvalues = len(values)
	assert(nvalues == m.Len())
	assert(nvalues == m2.Len())
	m.Release()
	assert(nvalues == m2.Len())
	assert(m.Len() == 0)
	for _, n := range values {
		assert(n == 1)
	}
	m2.Release()
	assert(len(values) == nvalues)
	assert(m2.Len() == 0)
	for _, n := range values {
		assert(n == 0)
	}
}

func TestMapStringKey(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))
	N := 10000
	var m Map[string, int]
	// m.StringPrefix = true
	var val int
	var ok bool
	for i := range N {
		val, ok = m.Get(strconv.Itoa(i))
		assert(!ok && val == 0)
		val, ok = m.Insert(strconv.Itoa(i), i)
		assert(ok && val == 0)
		val, ok = m.Get(strconv.Itoa(i))
		assert(ok && val == i)
		val, ok = m.Insert(strconv.Itoa(i), -i)
		assert(!ok && val == i)
		val, ok = m.Get(strconv.Itoa(i))
		assert(ok && val == i)
		m.tree.Sane(nil)
	}

	start := time.Now()
	for time.Since(start) < time.Second*3 {
		var m Map[string, int]
		// m.StringPrefix = true
		for i := range N {
			m.Insert(strconv.Itoa(i), i)
		}
		m.tree.Sane(nil)
		start := rng.Int() % N
		count := rng.Int() % (N / 3)
		if start+count > N {
			count = N - start
		}
		s := m.DeleteRangeAt(start, count)
		assert(s.Len() == count)
		assert(m.Len() == N-count)
		m.tree.Sane(nil)
	}
}

func itoa(i int) string {
	return strconv.Itoa(i)
}

func testBasic1String(t *testing.T, noprefix bool) {
	seed := uint64(time.Now().UnixNano())
	// seed = 1775141294651083000
	rng := rand.New(rand.NewPCG(0, seed))
	t.Logf("seed: %d", seed)
	N := 100000
	m := NewMapOptions(MapOptions[string, int]{
		// NoPrefix: noprefix,
	})

	val, ok := m.Get("")
	assert(!ok && val == 0)

	val, ok = m.Delete("")
	assert(!ok && val == 0)

	for h, i := range rng.Perm(N) {
		val, ok := m.Get(itoa(i))
		assert(!ok && val == 0)
		val, ok = m.GetMut(itoa(i))
		assert(!ok && val == 0)
		val, ok = m.Set(itoa(i), i)
		assert(!ok && val == 0)
		val, ok = m.Insert(itoa(i), i)
		assert(!ok && val == i)
		if h%301 == 0 {
			m.tree.Sane(nil)
		}
	}
	m.tree.Sane(nil)

	m2 := m.Copy()
	// assert(m2.tree.strprefix == !noprefix)
	assert(slices.Equal(slices.Collect(m.Keys()), slices.Collect(m2.Keys())))
	assert(slices.Equal(slices.Collect(m.Values()), slices.Collect(m2.Values())))
	assert(slices.Equal(slices.Collect(m.ValuesMut()), slices.Collect(m2.ValuesMut())))

	m.tree.Sane(nil)
	m2.tree.Sane(nil)

	for h, i := range rng.Perm(N) {
		val, ok := m2.Delete(itoa(i) + "*")
		assert(!ok && val == 0)
		if h%301 == 0 {
			m.tree.Sane(nil)
			m2.tree.Sane(nil)
		}
	}
	m.tree.Sane(nil)
	m2.tree.Sane(nil)

	for h, i := range rng.Perm(N) {
		val, ok := m.Delete(itoa(i))
		assert(ok && val == i)
		if h%301 == 0 {
			m.tree.Sane(nil)
			m2.tree.Sane(nil)
		}
	}
	m.tree.Sane(nil)
	m2.tree.Sane(nil)

	assert(!slices.Equal(slices.Collect(m.Keys()), slices.Collect(m2.Keys())))
	assert(!slices.Equal(slices.Collect(m.Values()), slices.Collect(m2.Values())))
	assert(!slices.Equal(slices.Collect(m.ValuesMut()), slices.Collect(m2.ValuesMut())))

	for h, i := range rng.Perm(N) {
		val, ok := m.Replace(itoa(i), i*100)
		assert(!ok && val == 0)
		if h%301 == 0 {
			m.tree.Sane(nil)
			m2.tree.Sane(nil)
		}

	}

	m.tree.Sane(nil)
	m2.tree.Sane(nil)

	for h, i := range rng.Perm(N) {
		val, ok := m2.Replace(itoa(i), i*100)
		assert(ok && val == i)
		if h%301 == 0 {
			m.tree.Sane(nil)
			m2.tree.Sane(nil)
		}
	}

	m.tree.Sane(nil)
	m2.tree.Sane(nil)

	for h, i := range rng.Perm(N) {
		val, ok := m2.Replace(itoa(i)+"*", i*100)
		assert(!ok && val == 0)
		if h%301 == 0 {
			m.tree.Sane(nil)
			m2.tree.Sane(nil)
		}
	}
}

func TestBasic1String(t *testing.T) {
	testBasic1String(t, false)
}
func TestBasic1StringNoPrefix(t *testing.T) {
	testBasic1String(t, true)
}

func TestBigStringKeys(t *testing.T) {
	prefix := "hfpqmrixjhshqoeuryfbal"

	m := NewMap[string, int]()

	for i := range 100000 {
		key := fmt.Sprintf("%s:%d", prefix, i)
		val, ok := m.Get(key)
		assert(!ok && val == 0)
		val, ok = m.GetMut(key)
		assert(!ok && val == 0)
		val, ok = m.Delete(key)
		assert(!ok && val == 0)
		val, ok = m.Set(key, i)
		assert(!ok && val == 0)
		val, ok = m.Get(key)
		assert(ok && val == i)
		val, ok = m.GetMut(key)
		assert(ok && val == i)
		val, ok = m.Set(key, -i)
		assert(ok && val == i)
		val, ok = m.Get(key)
		assert(ok && val == -i)
		if i%301 == 0 {
			m.tree.Sane(nil)
		}
	}
}

func TestSet(t *testing.T) {

	seed := uint64(time.Now().UnixNano())
	// seed = 1775853815283402000
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	m := make(map[int]bool)
	s := NewSet[int]()
	var keys []int
	for range 10000 {
		key := rng.Int() / 1000 * 1000
		ok := s.Insert(key)
		if !ok {
			assert(m[key])
		} else {
			m[key] = true
			keys = append(keys, key)
		}
		assert(s.Contains(key))
	}

	s.base.tree.Sane(nil)

	skeys := slices.Collect(s.All())
	s2 := s.Copy()

	slices.Sort(keys)
	slices.Reverse(keys)
	assert(slices.Equal(keys, slices.Collect(s.Backward())))
	slices.Sort(keys)
	assert(slices.Equal(keys, slices.Collect(s.All())))
	assert(slices.Equal(keys, slices.Collect(s2.All())))
	assert(slices.Equal(skeys, slices.Collect(s2.All())))
	assert(slices.Equal(skeys, keys))

	for i := range 100 {
		for key := range s.Ascend(keys[i]) {
			assert(key == keys[i])
			break
		}
	}
	for i := range 100 {
		j := len(keys) - i - 1
		for key := range s.Descend(keys[j]) {
			assert(key == keys[j])
			break
		}
	}

	for i := range 100 {
		for key := range s.AscendAt(i) {
			assert(key == keys[i])
			break
		}
	}
	for i := range 100 {
		j := len(keys) - i - 1
		for key := range s.DescendAt(j) {
			assert(key == keys[j])
			break
		}
	}

	v1, ok := s.GetAt(5000)
	assert(ok)
	i, ok := s.IndexOf(v1)
	assert(ok && i == 5000)

	v2, ok := s.Seek(v1 - 1)
	assert(ok && v2 == v1)

	v3, ok := s.SeekPrev(v1)
	assert(ok)
	v4, ok := s.GetAt(4999)
	assert(ok && v3 == v4)

	v3, ok = s.SeekNext(v1)
	assert(ok)
	v4, ok = s.GetAt(5001)
	assert(ok && v3 == v4)

	///

	assert(s.InsertAt(5001, v1+1))
	m[v1+1] = true
	v1, ok = s.GetAt(2500)
	assert(ok)
	v2, ok = s.DeleteAt(2500)
	assert(ok && v2 == v1)
	delete(m, v2)

	v3, ok = s.GetAt(2501)
	assert(ok)
	v4, ok = s.ReplaceAt(2501, v3+1)
	assert(ok && v3 == v4)
	delete(m, v3)
	m[v3+1] = true

	v1, ok = s.Front()
	assert(ok)
	v2, ok = s.GetAt(0)
	assert(ok && v1 == v2)
	assert(s.PushFront(v1 - 1))
	v2, ok = s.PopFront()
	assert(ok && v2 == v1-1)

	v1, ok = s.Back()
	assert(ok)
	v2, ok = s.GetAt(len(m) - 1)
	assert(ok && v1 == v2)
	assert(s.PushBack(v1 + 1))
	v2, ok = s.PopBack()
	assert(ok && v2 == v1+1)

	assert(s.base.tree.Height() > 0)

	var dkeys []int
	for key := range m {
		dkeys = append(dkeys, key)
	}
	for i := 0; i < 1000; i++ {
		assert(s.Delete(dkeys[i]))
		delete(m, dkeys[i])
	}

	k1, ok := s.GetAt(1000)
	assert(ok)
	k2, ok := s.GetAt(2000)
	assert(ok)
	deleted := slices.Collect(s.DeleteRange(k1, k2).All())
	assert(len(deleted) == 1000 && slices.IsSorted(deleted))
	for _, key := range deleted {
		delete(m, key)
	}

	deleted = slices.Collect(s.DeleteRangeAt(1000, 1000).All())
	assert(len(deleted) == 1000 && slices.IsSorted(deleted))
	for _, key := range deleted {
		delete(m, key)
	}

	for key := range m {
		assert(s.Delete(key))
	}

	assert(s.Len() == 0)

	assert(slices.Equal(skeys, slices.Collect(s2.All())))
	s3 := s2.Copy()
	s2.Release()
	assert(s2.Len() == 0)
	assert(slices.Equal(skeys, slices.Collect(s3.All())))
	s3.Clear()
	assert(s3.Len() == 0)
}

func TestSetDeleteRange(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))
	var b Set[int]
	for _, i := range rng.Perm(10000) {
		b.Insert(i)
	}
	b.base.tree.Sane(nil)
	assert(b.Len() == 10000)

	b2 := b.Copy()

	out := b.DeleteRange(1000, 6000)
	assert(out.Len() == 5000)

	b.base.tree.Sane(nil)
	assert(b.Len() == 5000)

	for _, i := range rng.Perm(10000) {
		b.Insert(i)
	}
	b.base.tree.Sane(nil)
	assert(b.Len() == 10000)

	out2 := b.DeleteRangeAt(1000, 5000)
	assert(out2.Len() == 5000)

	assert(len(slices.Collect(out2.All())) == 5000)
	assert(len(slices.Collect(out2.Backward())) == 5000)
	var i int
	for range out2.All() {
		if i == 100 {
			break
		}
		i++
	}
	i = 0
	for range out2.Backward() {
		if i == 100 {
			break
		}
		i++
	}

	b.base.tree.Sane(nil)
	assert(b.Len() == 5000)

	b2.base.tree.Sane(nil)
	assert(b2.Len() == 10000)

}

type RowA struct {
	id string `key:"0,asc"`
	// id string `key:"0,desc"`
}

type item[T any] struct{ key, val T }

func (itm *item[T]) empty() bool {
	var empty item[T]
	a := unsafe.Slice((*byte)(unsafe.Pointer(itm)), unsafe.Sizeof(empty))
	b := unsafe.Slice((*byte)(unsafe.Pointer(&empty)), unsafe.Sizeof(empty))
	return bytes.Equal(a, b)
}

func TestTable(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	isempty := func(_ omit, v item[int]) bool {
		return v.empty()
	}

	b := NewTableOptions(TableOptions[item[int]]{
		Compare: func(a, b item[int]) int {
			return cmp.Compare(a.key, b.key)
		},
	})

	keys := rng.Perm(100000)
	m := make(map[int]int)
	for i := range len(keys) {
		item := item[int]{keys[i], -keys[i]}
		x, ok := b.Get(item)
		assert(!ok && x.empty())
		x, ok = b.Insert(item)
		assert(ok && x.empty())
		m[item.key] = item.val
		x, ok = b.Get(item)
		assert(ok && x.key == keys[i] && x.val == -keys[i])
		if i%131 == 0 {
			b.tree.Sane(isempty)
		}
	}
	assert(len(m) == b.Len())
	b.tree.Sane(isempty)
	var i int
	var prev item[int]
	for item := range b.All() {
		if i > 0 {
			assert(item.key > prev.key)
			prev = item
		}
		if i == 100 {
			break
		}
		i++
	}

}

func testTableBasic1(t *testing.T, usecmp, useless bool) {
	type item struct{ key, val int }
	var m *Table[item]
	if usecmp {
		m = NewTableOptions(TableOptions[item]{
			Compare: func(a, b item) int { return cmp.Compare(a.key, b.key) },
		})
	} else if useless {
		m = NewTableOptions(TableOptions[item]{
			Less: func(a, b item) bool { return a.key < b.key },
		})
	} else {
		m = NewTable[item]()
	}

	itemIsEmpty := func(_ omit, v item) bool {
		return v.key == 0 && v.val == 0
	}
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))
	for i, j := range rng.Perm(100000) {
		key := j * 10

		val, found := m.Get(item{key, 0})
		assert(!found && val.val == 0)

		val, found = m.GetMut(item{key, 0})
		assert(!found && val.val == 0)

		assert(!m.Contains(item{key, 0}))

		old, deleted := m.Delete(item{key, 0})
		assert(!deleted && old.val == 0)

		old, replaced := m.Set(item{key, -key})
		assert(!replaced && old.val == 0)

		old, replaced = m.Set(item{key, -key})
		assert(replaced && old.val == -key)

		old, deleted = m.Delete(item{key, 0})
		assert(deleted && old.val == -key)

		old, replaced = m.Replace(item{key, key})
		assert(!replaced && old.val == 0)

		existing, inserted := m.Insert(item{key, -key})
		assert(inserted && existing.val == 0)

		existing, inserted = m.Insert(item{key, -key})
		assert(!inserted && existing.val == -key)

		old, replaced = m.Replace(item{key, key})
		assert(replaced && old.val == -key)

		old, replaced = m.Replace(item{key, -key})
		assert(replaced && old.val == key)

		val, found = m.Get(item{key, 0})
		assert(found && val.val == -key)

		assert(m.Contains(item{key, 0}))

		val2, found := m.Seek(item{key, 0})
		assert(found && val2.key == key && val2.val == -key)

		val2, found = m.Seek(item{key - 5, 0})
		assert(found && val2.key == key && val2.val == -key)

		val2, found = m.SeekNext(item{key, 0})
		if !found {
			val3, found := m.Back()
			assert(found && val3.key == key && val3.val == -key)
		} else {
			assert(val2.key > key && val2.val == -val2.key)
		}

		if i%131 == 0 {
			m.tree.Sane(itemIsEmpty)
		}
	}
	m.tree.Sane(itemIsEmpty)
}

func TestTableBasic1(t *testing.T) {
	testTableBasic1(t, false, false)
}

func TestTableBasic1Compare(t *testing.T) {
	testTableBasic1(t, true, false)
}

func TestTableBasic1Less(t *testing.T) {
	testTableBasic1(t, false, true)
}

func TestTableBadType(t *testing.T) {
	var b Table[struct{}]
	var ok bool
	_, ok = b.Insert(struct{}{})
	assert(ok == false)
	_, ok = b.Set(struct{}{})
	assert(ok == false)
	ok = b.PushFront(struct{}{})
	assert(ok == false)
	ok = b.PushBack(struct{}{})
	assert(ok == false)
}

func TestTableBasic2(t *testing.T) {
	type item struct{ key, val int }
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))
	N := 10000
	keys := rng.Perm(N)
	var c Table[item]
	assert(len(slices.Collect(c.All())) == 0)
	for i := range N {
		if c.Len() != i {
			t.Fatalf("expected %d got %d", i, c.Len())
		}

		val, found := c.Get(item{keys[i], 0})
		assert(!found && val.val == 0)

		val, inserted := c.Insert(item{keys[i], keys[i]})
		assert(inserted && val.val == 0)
		assert(c.Len() == i+1)
		c.tree.Sane(nil)

		val, found = c.Get(item{keys[i], 0})
		assert(found && val.val == keys[i])

		val, deleted := c.Delete(item{keys[i], 0})
		assert(deleted && val.val == keys[i])

		val, deleted = c.Delete(item{keys[i], 0})
		assert(!deleted && val.val == 0)

		val, inserted = c.Insert(item{keys[i], keys[i]})
		assert(inserted && val.val == 0)
		assert(c.Len() == i+1)
		c.tree.Sane(nil)

		val, deleted = c.Delete(item{keys[i], 0})
		assert(deleted && val.val == keys[i])

		val, deleted = c.Delete(item{keys[i], 0})
		assert(!deleted && val.val == 0)

		val, inserted = c.Insert(item{keys[i], keys[i]})
		assert(inserted && val.val == 0)
		assert(c.Len() == i+1)
		c.tree.Sane(nil)

		j, found := c.IndexOf(item{keys[i], 0})
		assert(found)

		val, found = c.GetAt(j)
		assert(found && val.key == keys[i] && val.val == keys[i])
	}
	var found bool
	val, found := c.GetAt(-1)
	assert(!found && val.key == 0 && val.val == 0)

	val, found = c.GetAt(N)
	assert(!found && val.key == 0 && val.val == 0)

	for i := range N {
		val, found := c.GetAt(i)
		assert(found && val.key == i && val.val == i)
	}
	// shuffle
	keys = rng.Perm(N)

	for i := range N {
		val, found := c.Get(item{keys[i], 0})
		assert(found && val.val == keys[i])

		val, inserted := c.Insert(item{keys[i], -keys[i]})
		assert(!inserted && val.val == keys[i])

		val, replaced := c.Replace(item{keys[i], -keys[i]})
		assert(replaced && val.val == keys[i])

		val, found = c.Get(item{keys[i], 0})
		assert(found && val.val == -keys[i])
		c.tree.Sane(nil)
	}

	for i := range N {
		val, found := c.GetAt(i)
		assert(found && val.key == i && val.val == -i)
	}

	// shuffle
	keys = rng.Perm(N)

	for i := range N {
		val, found := c.Get(item{keys[i], 0})
		assert(found && val.val == -keys[i])

		val, deleted := c.Delete(item{keys[i], 0})
		assert(deleted && val.val == -keys[i])

		val, found = c.Get(item{keys[i], 0})
		assert(!found && val.val == 0)

		val, deleted = c.Delete(item{keys[i], 0})
		assert(!deleted && val.val == 0)
		c.tree.Sane(nil)
	}

	// shuffle
	keys = rng.Perm(N)

	for i := range N {
		val, inserted := c.Insert(item{keys[i], -keys[i]})
		assert(inserted && val.val == 0)
	}
	v, replaced := c.ReplaceAt(-1, item{0, 0})
	assert(!replaced && v.key == 0 && v.val == 0)

	v, replaced = c.ReplaceAt(N+1, item{0, 0})
	assert(!replaced && v.key == 0 && v.val == 0)

	for i := range N {
		key2 := ((N - i) * 10)
		val, replaced := c.ReplaceAt(i, item{-key2, key2})
		assert(replaced && val.key == i && val.val == -val.key)
	}
	for i := range N {
		key2 := ((N - i) * 10)
		if i < N-1 {
			_, replaced := c.ReplaceAt(i, item{(-key2) + 1000, key2})
			assert(!replaced)
		}
		if i > 0 {
			_, replaced := c.ReplaceAt(i, item{(-key2) - 1000, key2})
			assert(!replaced)
		}
	}
	v, deleted := c.DeleteAt(-1)
	assert(!deleted && v.key == 0 && v.val == 0)

	for i := range N {
		val, deleted := c.DeleteAt(i)
		assert(deleted && val.val == -val.key)

		val, inserted := c.Insert(item{i, -i})
		assert(inserted && val.val == 0)
	}

	c.Clear()
	assert(c.Len() == 0)
}

type numeric interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64
}

func testTableVarious[T numeric](_ *testing.T) {
	b := NewTable[T]()
	m := make(map[T]bool)
	assert(b.PushFront(0))
	m[0] = true
	v, ok := b.Front()
	assert(ok && v == 0)
	v, ok = b.FrontMut()
	assert(ok && v == 0)
	for i := T(1); i < 120; i++ {
		assert(b.PushBack(i))
		m[i] = true
		v, ok := b.Back()
		assert(ok && v == i)
		v, ok = b.BackMut()
		assert(ok && v == i)
	}

	b.tree.Sane(nil)

	assert(b.Len() == 120)
	keys := slices.Sorted(maps.Keys(m))
	assert(slices.Equal(keys, slices.Collect(b.All())))
	assert(slices.Equal(keys, slices.Collect(b.AllMut())))

	slices.Reverse(keys)
	assert(slices.Equal(keys, slices.Collect(b.Backward())))
	assert(slices.Equal(keys, slices.Collect(b.BackwardMut())))
	assert(slices.Equal(keys[1:], slices.Collect(b.Descend(118))))
	assert(slices.Equal(keys[1:], slices.Collect(b.DescendMut(118))))
	assert(slices.Equal(keys[1:], slices.Collect(b.DescendAt(118))))
	assert(slices.Equal(keys[1:], slices.Collect(b.DescendAtMut(118))))

	slices.Reverse(keys)
	assert(slices.Equal(keys[1:], slices.Collect(b.Ascend(1))))
	assert(slices.Equal(keys[1:], slices.Collect(b.AscendMut(1))))
	assert(slices.Equal(keys[1:], slices.Collect(b.AscendAt(1))))
	assert(slices.Equal(keys[1:], slices.Collect(b.AscendAtMut(1))))

	for i := T(0); i < 120; i++ {
		v, ok := b.Seek(i)
		assert(ok && v == i)
		v, ok = b.SeekMut(i)
		assert(ok && v == i)
		if i < 119 {
			v, ok = b.SeekNext(i)
			assert(ok && v == i+1)
			v, ok = b.SeekNextMut(i)
			assert(ok && v == i+1)
		}
		if i > 0 {
			v, ok = b.SeekPrev(i)
			assert(ok && v == i-1)
			v, ok = b.SeekPrevMut(i)
			assert(ok && v == i-1)
		}

		v, ok = b.GetAt(int(i))
		assert(ok && v == i)
		v, ok = b.GetAtMut(int(i))
		assert(ok && v == i)
	}

	b.tree.Sane(nil)

	for i := T(0); i < 60; i++ {
		v, ok := b.PopFront()
		assert(ok && v == i)
	}

	b.tree.Sane(nil)

	for i := T(119); i >= 60; i-- {
		v, ok := b.PopBack()
		assert(ok && v == i)
	}

	b.tree.Sane(nil)
	assert(b.Len() == 0)

	b.Insert(1)
	assert(b.Len() == 1)
	b.InsertAt(0, 0)
	assert(b.Len() == 2)
	b.InsertAt(2, 2)
	assert(b.Len() == 3)
	b.Release()
	assert(b.Len() == 0)

	assert(b.tree.dataCompare(omit{}, 100, omit{}, 101) == -1)
	assert(b.tree.dataCompare(omit{}, 101, omit{}, 101) == 0)
	assert(b.tree.dataCompare(omit{}, 102, omit{}, 101) == 1)

	b = NewTableOptions(TableOptions[T]{
		Less: func(a, b T) bool {
			return a < b
		},
	})

	b.tree.Sane(nil)
	assert(b.Len() == 0)
	assert(b.tree.Height() == 0)

	b.Insert(1)
	assert(b.Len() == 1)

	b.InsertAt(0, 0)
	assert(b.Len() == 2)
	b.InsertAt(2, 2)
	assert(b.Len() == 3)
	b.Release()
	assert(b.Len() == 0)

	assert(b.tree.dataCompare(omit{}, 100, omit{}, 101) == -1)
	assert(b.tree.dataCompare(omit{}, 101, omit{}, 101) == 0)
	assert(b.tree.dataCompare(omit{}, 102, omit{}, 101) == 1)
}

func TestTableVarious(t *testing.T) {
	testTableVarious[int](t)
	testTableVarious[int8](t)
	testTableVarious[int16](t)
	testTableVarious[int32](t)
	testTableVarious[int64](t)
	testTableVarious[uint](t)
	testTableVarious[uint8](t)
	testTableVarious[uint16](t)
	testTableVarious[uint32](t)
	testTableVarious[uint64](t)
	testTableVarious[uintptr](t)
	testTableVarious[float32](t)
	testTableVarious[float64](t)
}

func TestTableDeleteRange(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))
	var b Table[int]
	for _, i := range rng.Perm(10000) {
		b.Insert(i)
	}
	b.tree.Sane(nil)
	assert(b.Len() == 10000)

	b2 := b.Copy()

	out := b.DeleteRange(1000, 6000)
	assert(out.Len() == 5000)

	b.tree.Sane(nil)
	assert(b.Len() == 5000)

	for _, i := range rng.Perm(10000) {
		b.Insert(i)
	}
	b.tree.Sane(nil)
	assert(b.Len() == 10000)

	out2 := b.DeleteRangeAt(1000, 5000)
	assert(out2.Len() == 5000)

	assert(len(slices.Collect(out2.All())) == 5000)
	assert(len(slices.Collect(out2.Backward())) == 5000)
	var i int
	for range out2.All() {
		if i == 100 {
			break
		}
		i++
	}
	i = 0
	for range out2.Backward() {
		if i == 100 {
			break
		}
		i++
	}

	b.tree.Sane(nil)
	assert(b.Len() == 5000)

	b2.tree.Sane(nil)
	assert(b2.Len() == 10000)

	assert((*slice[int])(nil).Len() == 0)
}

type testcmp[T any] struct {
	base ctype[T]
}

func (c *testcmp[T]) ok() bool {
	return c.base.compare != nil && c.base.search != nil
}

func (c *testcmp[T]) compare(a, b T) int {
	return c.base.compare(omit{}, a, omit{}, b)
}
func (c *testcmp[T]) search(arr []T, key T) (int, bool) {
	return c.base.search(len(arr), nil, unsafe.SliceData(arr), omit{}, key)
}

func maketestcmp[T any]() *testcmp[T] {
	var c testcmp[T]
	c.base = compareFor[T]()
	return &c
}

func testNumberComparator[T numeric]() {
	c := maketestcmp[T]()
	assert(c.ok())
	assert(c.compare(1, 2) == -1)
	assert(c.compare(2, 2) == 0)
	assert(c.compare(3, 2) == 1)
	arr := []T{10, 20, 30, 40, 50, 60, 70, 80, 90}
	i, found := c.search(arr, 50)
	assert(found && i == 4)
	i, found = c.search(arr, 51)
	assert(!found && i == 5)
	i, found = c.search(arr, 0)
	assert(!found && i == 0)
	i, found = c.search(arr, 99)
	assert(!found && i == 9)
}

func TestTableComparators(T *testing.T) {

	var t bool
	var x byte
	assert(unsafe.Sizeof(t) == unsafe.Sizeof(x))

	testNumberComparator[int]()
	testNumberComparator[int8]()
	testNumberComparator[int16]()
	testNumberComparator[int32]()
	testNumberComparator[int64]()
	testNumberComparator[uint]()
	testNumberComparator[uint8]()
	testNumberComparator[uint16]()
	testNumberComparator[uint32]()
	testNumberComparator[uint64]()
	testNumberComparator[uintptr]()
	testNumberComparator[float32]()
	testNumberComparator[float64]()

	{
		c := maketestcmp[string]()
		assert(c.ok())
		assert(c.compare("1", "2") == -1)
		assert(c.compare("2", "2") == 0)
		assert(c.compare("3", "2") == 1)
		arr := []string{"10", "20", "30", "40", "50", "60", "70", "80", "90"}
		i, found := c.search(arr, "50")
		assert(found && i == 4)
		i, found = c.search(arr, "51")
		assert(!found && i == 5)
		i, found = c.search(arr, "")
		assert(!found && i == 0)
		i, found = c.search(arr, "99")
		assert(!found && i == 9)
	}

	type ct struct{ string }

	{
		c := maketestcmp[ct]()
		assert(c.ok())
		assert(c.compare(ct{"1"}, ct{"2"}) == -1)
		assert(c.compare(ct{"2"}, ct{"2"}) == 0)
		assert(c.compare(ct{"3"}, ct{"2"}) == 1)
		arr := []ct{{"10"}, {"20"}, {"30"}, {"40"}, {"50"}, {"60"}, {"70"},
			{"80"}, {"90"}}
		i, found := c.search(arr, ct{"50"})
		assert(found && i == 4)
		i, found = c.search(arr, ct{"51"})
		assert(!found && i == 5)
		i, found = c.search(arr, ct{""})
		assert(!found && i == 0)
		i, found = c.search(arr, ct{"99"})
		assert(!found && i == 9)
	}
	{
		c := maketestcmp[*ct]()
		assert(c.ok())
		assert(c.compare(&ct{"1"}, &ct{"2"}) == -1)
		assert(c.compare(&ct{"2"}, &ct{"2"}) == 0)
		assert(c.compare(&ct{"3"}, &ct{"2"}) == 1)
		arr := []*ct{{"10"}, {"20"}, {"30"}, {"40"}, {"50"}, {"60"}, {"70"},
			{"80"}, {"90"}}
		i, found := c.search(arr, &ct{"50"})
		assert(found && i == 4)
		i, found = c.search(arr, &ct{"51"})
		assert(!found && i == 5)
		i, found = c.search(arr, &ct{""})
		assert(!found && i == 0)
		i, found = c.search(arr, &ct{"99"})
		assert(!found && i == 9)
	}
}

type dualList[T cmp.Ordered] struct {
	a []T
	b *Array[T]
}

func (a *dualList[T]) Insert(i int, item T) {
	a.a = slices.Insert(a.a, i, item)
	assert(a.b.Insert(i, item))
	item0 := a.a[i]
	item1, ok := a.b.Get(i)
	assert(ok && item0 == item1)
}

func (a *dualList[T]) PushFront(item T) {
	a.a = slices.Insert(a.a, 0, item)
	assert(a.b.PushFront(item))
	item0 := a.a[0]
	item1, ok := a.b.Front()
	assert(ok && item0 == item1)
}
func (a *dualList[T]) PushBack(item T) {
	i := len(a.a)
	a.a = slices.Insert(a.a, i, item)
	assert(a.b.PushBack(item))
	item0 := a.a[i]
	item1, ok := a.b.Back()
	assert(ok && item0 == item1)
}

func (a *dualList[T]) Replace(i int, item T) {
	item0 := a.a[i]
	item1, ok := a.b.Replace(i, item)
	assert(ok && item0 == item1)
	a.a[i] = item
}

func (a *dualList[T]) Delete(i int) {
	item0 := a.a[i]
	item1, ok := a.b.Delete(i)
	assert(ok && item0 == item1)
	a.a = slices.Delete(a.a, i, i+1)
}
func (a *dualList[T]) PopFront() {
	item0 := a.a[0]
	item1, ok := a.b.PopFront()
	assert(ok && item0 == item1)
	a.a = slices.Delete(a.a, 0, 1)
}
func (a *dualList[T]) PopBack() {
	i := len(a.a) - 1
	item0 := a.a[i]
	item1, ok := a.b.PopBack()
	assert(ok && item0 == item1)
	a.a = slices.Delete(a.a, i, i+1)
}

func (a *dualList[T]) Copy() *dualList[T] {
	b := new(dualList[T])
	b.a = make([]T, len(a.a))
	copy(b.a, a.a)
	b.b = a.b.Copy()
	return b
}

func TestList(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	// seed = 1775853815283402000
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var a dualList[int]
	a.b = NewList[int]()
	for i := range 10000 {
		switch rng.Int() % 100 {
		case 0:
			a.PushFront(rng.Int())
		case 1:
			a.PushBack(rng.Int())
		default:
			a.Insert(rng.IntN(i+1), rng.Int())
		}
		a.Replace(rng.IntN(i+1), rng.Int())

	}
	a.b.tree.Sane(nil)

	assert(slices.Equal(a.a, slices.Collect(a.b.All())))

	a2 := a.Copy()
	a2all := slices.Collect(a2.b.All())

	assert(slices.Equal(a2.a, slices.Collect(a2.b.All())))

	deleted := make([]int, 5000)
	copy(deleted, a.a[2500:7500])
	a.a = slices.Delete(a.a, 2500, 7500)
	var deleted2 []int
	deleted2 = slices.Collect(a.b.DeleteRange(2500, 2500).All())
	for range 2500 {
		item, ok := a.b.Delete(2500)
		assert(ok)
		deleted2 = append(deleted2, item)
	}
	assert(slices.Equal(deleted, deleted2))
	a.b.tree.Sane(nil)

	assert(slices.Equal(a2.a, slices.Collect(a2.b.All())))
	// return

	assert(len(a.a) == a.b.Len() && len(a.a) == 5000)
	assert(a.b.tree.Height() > 0)

	for range 5000 {
		switch rng.Int() % 100 {
		case 0:
			a.PopFront()
		case 1:
			a.PopBack()
		default:
			a.Delete(rng.IntN(len(a.a)))
		}
	}

	assert(len(a.a) == a.b.Len() && len(a.a) == 0)
	assert(a.b.tree.Height() == 0)

	for i := range 5000 {
		a.Insert(rng.IntN(i+1), rng.Int())
	}
	a.a = nil
	a.b.Clear()
	assert(len(a.a) == a.b.Len() && len(a.a) == 0)
	assert(a.b.tree.Height() == 0)

	for i := range 5000 {
		a.Insert(rng.IntN(i+1), rng.Int())
	}
	a.a = nil
	a.b.Release()
	assert(len(a.a) == a.b.Len() && len(a.a) == 0)
	assert(a.b.tree.Height() == 0)

	//////////

	a = *a2
	a2 = nil

	a.b.tree.Sane(nil)

	assert(len(a.a) == a.b.Len() && len(a.a) == 10000)
	assert(a.b.tree.Height() > 0)

	assert(slices.Equal(a.a, slices.Collect(a.b.All())))
	assert(slices.Equal(a.a, a2all))

	slices.Reverse(a.a)
	assert(slices.Equal(a.a, slices.Collect(a.b.Backward())))
	slices.Reverse(a.a)

	assert(slices.Equal(a.a[2500:], slices.Collect(a.b.Ascend(2500))))
	slices.Reverse(a.a)
	assert(slices.Equal(a.a[2500:], slices.Collect(a.b.Descend(7499))))
	slices.Reverse(a.a)

}

func TestStack(t *testing.T) {
	b := NewStack[int]()
	b.Push(20)
	b.Push(10)
	b.Push(30)
	b.Push(50)
	b.Push(40)
	assert(slices.Equal([]int{40, 50, 30, 10, 20}, slices.Collect(b.All())))
	assert(b.Len() == 5)

	var x int
	var ok bool
	x, ok = b.At(0)
	assert(ok && x == 40)
	x, ok = b.At(1)
	assert(ok && x == 50)
	x, ok = b.At(2)
	assert(ok && x == 30)
	x, ok = b.At(3)
	assert(ok && x == 10)
	x, ok = b.At(4)
	assert(ok && x == 20)
	x, ok = b.At(5)
	assert(!ok && x == 0)
	x, ok = b.AtMut(0)
	assert(ok && x == 40)
	x, ok = b.AtMut(1)
	assert(ok && x == 50)
	x, ok = b.AtMut(2)
	assert(ok && x == 30)
	x, ok = b.AtMut(3)
	assert(ok && x == 10)
	x, ok = b.AtMut(4)
	assert(ok && x == 20)
	x, ok = b.AtMut(5)
	assert(!ok && x == 0)

	x, ok = b.Top()
	assert(ok && x == 40)
	x, ok = b.TopMut()
	assert(ok && x == 40)
	x, ok = b.Pop()
	assert(ok && x == 40)
	assert(b.Len() == 4)

	x, ok = b.Top()
	assert(ok && x == 50)
	x, ok = b.TopMut()
	assert(ok && x == 50)
	x, ok = b.Pop()
	assert(ok && x == 50)
	assert(b.Len() == 3)

	x, ok = b.Top()
	assert(ok && x == 30)
	x, ok = b.TopMut()
	assert(ok && x == 30)
	x, ok = b.Pop()
	assert(ok && x == 30)
	assert(b.Len() == 2)

	b2 := b.Copy()
	assert(b2.Len() == 2)
	b3 := b.Copy()
	assert(b3.Len() == 2)

	x, ok = b.Top()
	assert(ok && x == 10)
	x, ok = b.TopMut()
	assert(ok && x == 10)
	x, ok = b.Pop()
	assert(ok && x == 10)
	assert(b.Len() == 1)

	x, ok = b.Top()
	assert(ok && x == 20)
	x, ok = b.TopMut()
	assert(ok && x == 20)
	x, ok = b.Pop()
	assert(ok && x == 20)
	assert(b.Len() == 0)

	x, ok = b.Top()
	assert(!ok && x == 0)
	x, ok = b.TopMut()
	assert(!ok && x == 0)
	x, ok = b.Pop()
	assert(!ok && x == 0)

	assert(b2.Len() == 2 && b3.Len() == 2)
	b2.Pop()
	assert(b2.Len() == 1 && b3.Len() == 2)
	b2.Clear()
	assert(b2.Len() == 0 && b3.Len() == 2)
	b3.Release()
	assert(b2.Len() == 0 && b3.Len() == 0)
}

func TestQueue(T *testing.T) {
	b := NewQueue[int]()
	b.Push(20)
	b.Push(10)
	b.Push(30)
	b.Push(50)
	b.Push(40)
	assert(slices.Equal([]int{20, 10, 30, 50, 40}, slices.Collect(b.All())))
	assert(b.Len() == 5)

	var x int
	var ok bool
	x, ok = b.At(0)
	assert(ok && x == 20)
	x, ok = b.At(1)
	assert(ok && x == 10)
	x, ok = b.At(2)
	assert(ok && x == 30)
	x, ok = b.At(3)
	assert(ok && x == 50)
	x, ok = b.At(4)
	assert(ok && x == 40)
	x, ok = b.At(5)
	assert(!ok && x == 0)
	x, ok = b.AtMut(0)
	assert(ok && x == 20)
	x, ok = b.AtMut(1)
	assert(ok && x == 10)
	x, ok = b.AtMut(2)
	assert(ok && x == 30)
	x, ok = b.AtMut(3)
	assert(ok && x == 50)
	x, ok = b.AtMut(4)
	assert(ok && x == 40)
	x, ok = b.AtMut(5)
	assert(!ok && x == 0)

	x, ok = b.Front()
	assert(ok && x == 20)
	x, ok = b.FrontMut()
	assert(ok && x == 20)
	x, ok = b.Pop()
	assert(ok && x == 20)
	assert(b.Len() == 4)

	x, ok = b.Front()
	assert(ok && x == 10)
	x, ok = b.FrontMut()
	assert(ok && x == 10)
	x, ok = b.Pop()
	assert(ok && x == 10)
	assert(b.Len() == 3)

	x, ok = b.Front()
	assert(ok && x == 30)
	x, ok = b.FrontMut()
	assert(ok && x == 30)
	x, ok = b.Pop()
	assert(ok && x == 30)
	assert(b.Len() == 2)

	b2 := b.Copy()
	assert(b2.Len() == 2)
	b3 := b.Copy()
	assert(b3.Len() == 2)

	x, ok = b.Front()
	assert(ok && x == 50)
	x, ok = b.FrontMut()
	assert(ok && x == 50)
	x, ok = b.Pop()
	assert(ok && x == 50)
	assert(b.Len() == 1)

	x, ok = b.Front()
	assert(ok && x == 40)
	x, ok = b.FrontMut()
	assert(ok && x == 40)
	x, ok = b.Pop()
	assert(ok && x == 40)
	assert(b.Len() == 0)

	x, ok = b.Front()
	assert(!ok && x == 0)
	x, ok = b.FrontMut()
	assert(!ok && x == 0)
	x, ok = b.Pop()
	assert(!ok && x == 0)

	assert(b2.Len() == 2 && b3.Len() == 2)
	b2.Pop()
	assert(b2.Len() == 1 && b3.Len() == 2)
	b2.Clear()
	assert(b2.Len() == 0 && b3.Len() == 2)
	b3.Release()
	assert(b2.Len() == 0 && b3.Len() == 0)

}

func randO[T any](rng *rand.Rand) T {
	var empty T
	switch any(empty).(type) {
	case int:
		return direct[int, T](int(rng.Int()))
	case int8:
		return direct[int8, T](int8(rng.Int()))
	case int16:
		return direct[int16, T](int16(rng.Int()))
	case int32:
		return direct[int32, T](int32(rng.Int()))
	case int64:
		return direct[int64, T](int64(rng.Int()))
	case uint:
		return direct[uint, T](uint(rng.Int()))
	case uint8:
		return direct[uint8, T](uint8(rng.Int()))
	case uint16:
		return direct[uint16, T](uint16(rng.Int()))
	case uint32:
		return direct[uint32, T](uint32(rng.Int()))
	case uint64:
		return direct[uint64, T](uint64(rng.Int()))
	case uintptr:
		return direct[uintptr, T](uintptr(rng.Int()))
	case float32:
		return direct[float32, T](float32(rng.Int()))
	case float64:
		return direct[float64, T](float64(rng.Int()))
	case string:
		return direct[string, T](fmt.Sprint(rng.Int()))
	}
	return empty
}

func wrapCompare[T any](c ctype[T]) func(T, T) int {
	return func(a, b T) int {
		return c.compare(omit{}, a, omit{}, b)
	}
}

func wrapSearch[T any](c ctype[T]) func([]T, T) (int, bool) {
	return func(data []T, key T) (int, bool) {
		return c.search(len(data), nil, unsafe.SliceData(data), omit{}, key)
	}
}

func wrapCompareFor[T any]() (x func(T, T) int, y func([]T, T) (int, bool)) {
	c := compareFor[T]()
	if c.compare != nil {
		x = wrapCompare(c)
	}
	if c.search != nil {
		y = wrapSearch(c)
	}
	return x, y
}

func testCompareBasicType[O cmp.Ordered](rng *rand.Rand) {
	var vals []O
	for range 1000 {
		vals = append(vals, randO[O](rng))
	}
	slices.Sort(vals)
	c := compareFor[O]()
	assert(c.compare != nil && c.search != nil)
	compare := wrapCompare(c)
	search := wrapSearch(c)
	for i := range len(vals) {
		x := randO[O](rng)
		c0 := cmp.Compare(x, vals[i])
		c1 := compare(x, vals[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearch(vals, x)
		i1, f1 := search(vals, x)
		assert(i0 == i1)
		assert(f0 == f1)
	}
}

func TestCompareBasicTypes(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))
	testCompareBasicType[int](rng)
	testCompareBasicType[int8](rng)
	testCompareBasicType[int16](rng)
	testCompareBasicType[int32](rng)
	testCompareBasicType[int64](rng)
	testCompareBasicType[uint](rng)
	testCompareBasicType[uint8](rng)
	testCompareBasicType[uint16](rng)
	testCompareBasicType[uint32](rng)
	testCompareBasicType[uint64](rng)
	testCompareBasicType[float32](rng)
	testCompareBasicType[float64](rng)
	testCompareBasicType[string](rng)
	testCompareBasicType[uintptr](rng)
	testCompareBasicType[uintptr](rng)
}
func TestCompareStructSingleField(t *testing.T) {
	type Row struct {
		id int
	}

	compare0 := func(a, b Row) int {
		return cmp.Compare(a.id, b.id)
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []Row
	for i := 0; i < 1000; i++ {
		rows = append(rows, Row{id: rng.Int()})
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := Row{id: rng.Int()}
		c0 := compare0(x, rows[i])
		c1 := compare(x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, x, compare0)
		i1, f1 := search(rows, x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}

func TestCompareStructSingleFieldIndirect(t *testing.T) {
	type Row struct {
		id int
	}

	compare0 := func(a, b *Row) int {
		return cmp.Compare(a.id, b.id)
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []*Row
	for i := 0; i < 1000; i++ {
		row := new(Row)
		row.id = rng.Int()
		rows = append(rows, row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[*Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = rng.Int()
		c0 := compare0(x, rows[i])
		c1 := compare(x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, x, compare0)
		i1, f1 := search(rows, x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}

func TestCompareStructSingleKeyed(t *testing.T) {
	type Row struct {
		id int `btree:"key"`
	}

	compare0 := func(a, b *Row) int {
		return cmp.Compare(a.id, b.id)
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []*Row
	for range 1000 {
		row := new(Row)
		row.id = rng.Int()
		rows = append(rows, row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[*Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = rng.Int()
		c0 := compare0(x, rows[i])
		c1 := compare(x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, x, compare0)
		i1, f1 := search(rows, x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}

func TestCompareStructAsc(t *testing.T) {
	type Row struct {
		id int `btree:"key,asc"`
	}

	compare0 := func(a, b *Row) int {
		return cmp.Compare(a.id, b.id)
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []*Row
	for range 1000 {
		row := new(Row)
		row.id = rng.Int()
		rows = append(rows, row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[*Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = rng.Int()
		c0 := compare0(x, rows[i])
		c1 := compare(x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, x, compare0)
		i1, f1 := search(rows, x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}
func TestCompareStructDesc(t *testing.T) {
	type Row struct {
		id int `btree:"key,desc"`
	}

	compare0 := func(a, b *Row) int {
		return cmp.Compare(b.id, a.id)
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []*Row
	for range 1000 {
		row := new(Row)
		row.id = rng.Int()
		rows = append(rows, row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[*Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = rng.Int()
		c0 := compare0(x, rows[i])
		c1 := compare(x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, x, compare0)
		i1, f1 := search(rows, x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}

func TestCompareStructNoFields(t *testing.T) {
	type Row struct{}
	compare, search := wrapCompareFor[Row]()
	assert(compare == nil && search == nil)
}

func randString(rng *rand.Rand, n int) string {
	s := make([]byte, n)
	for i := 0; i < n; i++ {
		s[i] = byte((rng.Int() % 64) + 'A')
	}
	return string(s)
}

func TestCompareCollateBinaryCS(t *testing.T) {
	type Row struct {
		id string `btree:"key,binary_CS"`
	}

	compare0 := func(a, b *Row) int {
		return cmp.Compare(a.id, b.id)
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []*Row
	for range 1000 {
		row := new(Row)
		row.id = randString(rng, 20)
		rows = append(rows, row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[*Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = randString(rng, rng.Int()%30)
		c0 := compare0(x, rows[i])
		c1 := compare(x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, x, compare0)
		i1, f1 := search(rows, x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}

func TestCompareCollateBinaryCI(t *testing.T) {
	type Row struct {
		id string `btree:"key,binary_CI"`
	}

	compare0 := func(a, b *Row) int {
		return cmp.Compare(strings.ToLower(a.id), strings.ToLower(b.id))
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []*Row
	for range 1000 {
		row := new(Row)
		row.id = randString(rng, 20)
		rows = append(rows, row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[*Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = randString(rng, rng.Int()%30)
		c0 := compare0(x, rows[i])
		c1 := compare(x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, x, compare0)
		i1, f1 := search(rows, x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}

func TestCompareCollateBinaryCIDirect(t *testing.T) {
	type Row struct {
		id string `btree:"key,binary_CI"`
	}

	compare0 := func(a, b Row) int {
		return cmp.Compare(strings.ToLower(a.id), strings.ToLower(b.id))
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []Row
	for range 1000 {
		row := new(Row)
		row.id = randString(rng, 20)
		rows = append(rows, *row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = randString(rng, rng.Int()%30)
		c0 := compare0(*x, rows[i])
		c1 := compare(*x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, *x, compare0)
		i1, f1 := search(rows, *x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}

func TestCompareStringDescDirect(t *testing.T) {
	type Row struct {
		id string `btree:"key,desc"`
	}

	compare0 := func(a, b Row) int {
		return cmp.Compare(b.id, a.id)
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []Row
	for range 1000 {
		row := new(Row)
		row.id = randString(rng, 20)
		rows = append(rows, *row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = randString(rng, rng.Int()%30)
		c0 := compare0(*x, rows[i])
		c1 := compare(*x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, *x, compare0)
		i1, f1 := search(rows, *x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}
func TestCompareStringDescDirectMultiKey(t *testing.T) {
	type Row struct {
		id  string `btree:"key,desc"`
		id2 int    `btree:"key"`
	}

	compare0 := func(a, b Row) int {
		c := cmp.Compare(b.id, a.id)
		if c == 0 {
			c = cmp.Compare(a.id2, b.id2)
		}
		return c
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []Row
	for range 1000 {
		row := new(Row)
		row.id = randString(rng, 3)
		row.id2 = rng.Int()
		rows = append(rows, *row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = randString(rng, 2)
		x.id2 = rng.Int()
		c0 := compare0(*x, rows[i])
		c1 := compare(*x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, *x, compare0)
		i1, f1 := search(rows, *x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}

func TestCompareCollateUtf8CS(t *testing.T) {
	type Row struct {
		id string `btree:"key,utf8_CS"`
	}

	compare0 := func(a, b *Row) int {
		return cmp.Compare(a.id, b.id)
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []*Row
	for range 1000 {
		row := new(Row)
		row.id = randString(rng, 20)
		rows = append(rows, row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[*Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = randString(rng, rng.Int()%30)
		c0 := compare0(x, rows[i])
		c1 := compare(x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, x, compare0)
		i1, f1 := search(rows, x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}

func TestCompareCollateUtf8CI(t *testing.T) {
	type Row struct {
		id string `btree:"key,utf8_CI"`
	}

	compare0 := func(a, b *Row) int {
		return cmp.Compare(strings.ToLower(a.id), strings.ToLower(b.id))
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []*Row
	for range 1000 {
		row := new(Row)
		row.id = randString(rng, 20)
		rows = append(rows, row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[*Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = randString(rng, rng.Int()%30)
		c0 := compare0(x, rows[i])
		c1 := compare(x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, x, compare0)
		i1, f1 := search(rows, x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}
func TestCompareStructMultiKeyed(t *testing.T) {
	type Row struct {
		id  int `btree:"key"`
		id2 int `btree:"key"`
	}

	compare0 := func(a, b *Row) int {
		c := cmp.Compare(a.id, b.id)
		if c == 0 {
			c = cmp.Compare(a.id2, b.id2)
		}
		return c
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []*Row
	for range 1000 {
		row := new(Row)
		row.id = rng.Int()
		rows = append(rows, row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[*Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = rng.Int()
		x.id2 = rng.Int()
		c0 := compare0(x, rows[i])
		c1 := compare(x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, x, compare0)
		i1, f1 := search(rows, x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}

func TestCompareStructMultiKeyed2(t *testing.T) {
	type Row struct {
		id  int `btree:"key.0"`
		id2 int `btree:"key"`
	}

	compare0 := func(a, b *Row) int {
		c := cmp.Compare(a.id, b.id)
		if c == 0 {
			c = cmp.Compare(a.id2, b.id2)
		}
		return c
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []*Row
	for range 1000 {
		row := new(Row)
		row.id = rng.Int()
		rows = append(rows, row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[*Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = rng.Int()
		x.id2 = rng.Int()
		c0 := compare0(x, rows[i])
		c1 := compare(x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, x, compare0)
		i1, f1 := search(rows, x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}

func TestCompareStructMultiKeyed3(t *testing.T) {
	type Row struct {
		id  int `btree:"key.0"`
		id2 int `btree:"key.928374"`
	}

	compare0 := func(a, b *Row) int {
		c := cmp.Compare(a.id, b.id)
		if c == 0 {
			c = cmp.Compare(a.id2, b.id2)
		}
		return c
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []*Row
	for range 1000 {
		row := new(Row)
		row.id = rng.Int()
		rows = append(rows, row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[*Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = rng.Int()
		x.id2 = rng.Int()
		c0 := compare0(x, rows[i])
		c1 := compare(x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, x, compare0)
		i1, f1 := search(rows, x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}

func TestCompareStructMultiKeyed4(t *testing.T) {
	type Row struct {
		id  int `btree:"key.-987"`
		id2 int `btree:"key.928374"`
	}

	compare0 := func(a, b *Row) int {
		c := cmp.Compare(a.id, b.id)
		if c == 0 {
			c = cmp.Compare(a.id2, b.id2)
		}
		return c
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []*Row
	for range 1000 {
		row := new(Row)
		row.id = rng.Int()
		rows = append(rows, row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[*Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = rng.Int()
		x.id2 = rng.Int()
		c0 := compare0(x, rows[i])
		c1 := compare(x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, x, compare0)
		i1, f1 := search(rows, x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}

func TestCompareStructMultiKeyed5(t *testing.T) {
	type Row struct {
		id2 int `btree:"key.928374"`
		id  int `btree:"key.-987"`
	}

	compare0 := func(a, b *Row) int {
		c := cmp.Compare(a.id, b.id)
		if c == 0 {
			c = cmp.Compare(a.id2, b.id2)
		}
		return c
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []*Row
	for range 1000 {
		row := new(Row)
		row.id = rng.Int()
		rows = append(rows, row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[*Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = rng.Int()
		x.id2 = rng.Int()
		c0 := compare0(x, rows[i])
		c1 := compare(x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, x, compare0)
		i1, f1 := search(rows, x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}

func TestCompareStructMultiKeyed6(t *testing.T) {
	type Row struct {
		id2 int `btree:"key.owiquery"`
		id  int `btree:"key.-987"`
	}

	compare0 := func(a, b *Row) int {
		c := cmp.Compare(a.id, b.id)
		if c == 0 {
			c = cmp.Compare(a.id2, b.id2)
		}
		return c
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []*Row
	for range 1000 {
		row := new(Row)
		row.id = rng.Int()
		rows = append(rows, row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[*Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = rng.Int()
		x.id2 = rng.Int()
		c0 := compare0(x, rows[i])
		c1 := compare(x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, x, compare0)
		i1, f1 := search(rows, x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}

func TestCompareStructMultiKeyedDirect(t *testing.T) {
	type Row struct {
		id  int `btree:"key"`
		id2 int `btree:"key"`
	}

	compare0 := func(a, b Row) int {
		c := cmp.Compare(a.id, b.id)
		if c == 0 {
			c = cmp.Compare(a.id2, b.id2)
		}
		return c
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []Row
	for range 1000 {
		row := new(Row)
		row.id = rng.Int()
		rows = append(rows, *row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = rng.Int()
		x.id2 = rng.Int()
		c0 := compare0(*x, rows[i])
		c1 := compare(*x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, *x, compare0)
		i1, f1 := search(rows, *x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}

func TestCompareUTF8(t *testing.T) {
	assert(compareUtf8("a", "A", false) == 1)
	assert(compareUtf8("A", "a", false) == -1)
	assert(compareUtf8("A", "A", false) == 0)
	assert(compareUtf8("a", "A", true) == 0)
	assert(compareUtf8("aa", "A", true) == 1)
	assert(compareUtf8("a", "AA", true) == -1)
}

func dedup[T any](rows []T, compare func(T, T) int) []T {
	// rows is already sorted
	var rows2 []T
	for i := range rows {
		if len(rows2) == 0 || compare(rows2[len(rows2)-1], rows[i]) < 0 {
			rows2 = append(rows2, rows[i])
		}
	}
	return rows2
}

func testCompareMultiKeyedI[T int | int8 | int16 | int32 | int64 |
	uint | uint8 | uint16 | uint32 | uint64 | uintptr | float32 | float64,
](t *testing.T) {
	type Row struct {
		id  T   `btree:"key"`
		id2 int `btree:"key"`
	}

	compare0 := func(a, b *Row) int {
		c := cmp.Compare(a.id, b.id)
		if c == 0 {
			c = cmp.Compare(a.id2, b.id2)
		}
		return c
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []*Row
	for range 1000 {
		row := new(Row)
		row.id = T(rng.Int())
		rows = append(rows, row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[*Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := new(Row)
		x.id = T(rng.Int())
		x.id2 = rng.Int()
		c0 := compare0(x, rows[i])
		c1 := compare(x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, x, compare0)
		i1, f1 := search(rows, x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}

func TestCompareMultiKeyedVarious(t *testing.T) {
	t.Run("int", func(t *testing.T) { testCompareMultiKeyedI[int](t) })
	t.Run("int8", func(t *testing.T) { testCompareMultiKeyedI[int8](t) })
	t.Run("int16", func(t *testing.T) { testCompareMultiKeyedI[int16](t) })
	t.Run("int32", func(t *testing.T) { testCompareMultiKeyedI[int32](t) })
	t.Run("int64", func(t *testing.T) { testCompareMultiKeyedI[int64](t) })
	t.Run("uint", func(t *testing.T) { testCompareMultiKeyedI[uint](t) })
	t.Run("uint8", func(t *testing.T) { testCompareMultiKeyedI[uint8](t) })
	t.Run("uint16", func(t *testing.T) { testCompareMultiKeyedI[uint16](t) })
	t.Run("uint32", func(t *testing.T) { testCompareMultiKeyedI[uint32](t) })
	t.Run("uint64", func(t *testing.T) { testCompareMultiKeyedI[uint64](t) })
	t.Run("uintptr", func(t *testing.T) { testCompareMultiKeyedI[uintptr](t) })
	t.Run("float32", func(t *testing.T) { testCompareMultiKeyedI[float32](t) })
	t.Run("float64", func(t *testing.T) { testCompareMultiKeyedI[float64](t) })
}

func testCompareSingleKeyedD[T int | int8 | int16 | int32 | int64 |
	uint | uint8 | uint16 | uint32 | uint64 | uintptr | float32 | float64,
](t *testing.T) {
	type Row struct {
		id2 int
		id  T `btree:"key"`
	}

	compare0 := func(a, b Row) int {
		c := cmp.Compare(a.id, b.id)
		return c
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var rows []Row
	for range 1000 {
		row := Row{id: T(rng.Int())}
		rows = append(rows, row)
	}
	slices.SortFunc(rows, compare0)
	rows = dedup(rows, compare0)

	compare, search := wrapCompareFor[Row]()
	assert(compare != nil && search != nil)

	for i := range len(rows) {
		x := Row{id: T(rng.Int()), id2: rng.Int()}
		c0 := compare0(x, rows[i])
		c1 := compare(x, rows[i])
		assert(c0 == c1)
		i0, f0 := slices.BinarySearchFunc(rows, x, compare0)
		i1, f1 := search(rows, x)
		assert(i0 == i1)
		assert(f0 == f1)
		i0, f0 = slices.BinarySearchFunc(rows, rows[i], compare0)
		i1, f1 = search(rows, rows[i])
		assert(i0 == i && f0 && i0 == i1 && f0 == f1)
	}
}

func TestCompareSingleKeyedVarious(t *testing.T) {
	t.Run("int", func(t *testing.T) { testCompareSingleKeyedD[int](t) })
	t.Run("int8", func(t *testing.T) { testCompareSingleKeyedD[int8](t) })
	t.Run("int16", func(t *testing.T) { testCompareSingleKeyedD[int16](t) })
	t.Run("int32", func(t *testing.T) { testCompareSingleKeyedD[int32](t) })
	t.Run("int64", func(t *testing.T) { testCompareSingleKeyedD[int64](t) })
	t.Run("uint", func(t *testing.T) { testCompareSingleKeyedD[uint](t) })
	t.Run("uint8", func(t *testing.T) { testCompareSingleKeyedD[uint8](t) })
	t.Run("uint16", func(t *testing.T) { testCompareSingleKeyedD[uint16](t) })
	t.Run("uint32", func(t *testing.T) { testCompareSingleKeyedD[uint32](t) })
	t.Run("uint64", func(t *testing.T) { testCompareSingleKeyedD[uint64](t) })
	t.Run("uintptr", func(t *testing.T) { testCompareSingleKeyedD[uintptr](t) })
	t.Run("float32", func(t *testing.T) { testCompareSingleKeyedD[float32](t) })
	t.Run("float64", func(t *testing.T) { testCompareSingleKeyedD[float64](t) })
}

func TestCompareSearchString(t *testing.T) {
	type Row struct{ string }
	_, search := wrapCompareFor[Row]()
	var rows []Row
	for i := range 100 {
		rows = append(rows, Row{fmt.Sprintf("%03d0", i)})
	}
	i, ok := search(rows, Row{""})
	assert(!ok && i == 0)
	i, ok = search(rows, Row{"0519"})
	assert(!ok && i == 52)
	i, ok = search(rows, Row{"0520"})
	assert(ok && i == 52)
	i, ok = search(rows, Row{"0521"})
	assert(!ok && i == 53)
	i, ok = search(rows, Row{"0991"})
	assert(!ok && i == len(rows))
}

func TestCompareSearchStringFieldsD(t *testing.T) {
	type Row struct {
		a string `btree:"key"`
		b int    `btree:"key"`
	}
	_, search := wrapCompareFor[Row]()
	var rows []Row
	for i := range 100 {
		rows = append(rows, Row{fmt.Sprintf("%03d0", i), 5})
	}
	i, ok := search(rows, Row{"", 5})
	assert(!ok && i == 0)
	i, ok = search(rows, Row{"0519", 5})
	assert(!ok && i == 52)
	i, ok = search(rows, Row{"0520", 5})
	assert(ok && i == 52)
	i, ok = search(rows, Row{"0521", 5})
	assert(!ok && i == 53)
	i, ok = search(rows, Row{"0991", 5})
	assert(!ok && i == len(rows))

	i, ok = search(rows, Row{"0520", 4})
	assert(!ok && i == 52)
	i, ok = search(rows, Row{"0520", 6})
	assert(!ok && i == 53)

}

func TestCompareSearchInt(t *testing.T) {
	type Row struct{ int }
	_, search := wrapCompareFor[Row]()
	var rows []Row
	for i := range 100 {
		rows = append(rows, Row{i * 10})
	}
	i, ok := search(rows, Row{-1})
	assert(!ok && i == 0)
	i, ok = search(rows, Row{519})
	assert(!ok && i == 52)
	i, ok = search(rows, Row{520})
	assert(ok && i == 52)
	i, ok = search(rows, Row{521})
	assert(!ok && i == 53)
	i, ok = search(rows, Row{991})
	assert(!ok && i == len(rows))
}

func TestCompareSearchIntI(t *testing.T) {
	type Row struct{ int }
	_, search := wrapCompareFor[*Row]()
	var rows []*Row
	for i := range 100 {
		rows = append(rows, &Row{i * 10})
	}
	i, ok := search(rows, &Row{-1})
	assert(!ok && i == 0)
	i, ok = search(rows, &Row{519})
	assert(!ok && i == 52)
	i, ok = search(rows, &Row{520})
	assert(ok && i == 52)
	i, ok = search(rows, &Row{521})
	assert(!ok && i == 53)
	i, ok = search(rows, &Row{991})
	assert(!ok && i == len(rows))
}

func TestCompareSearchIntFieldsD(t *testing.T) {
	type Row struct {
		a int `btree:"key"`
		b int `btree:"key"`
	}
	_, search := wrapCompareFor[Row]()
	var rows []Row
	for i := range 100 {
		rows = append(rows, Row{i * 10, 5})
	}
	i, ok := search(rows, Row{-1, 5})
	assert(!ok && i == 0)
	i, ok = search(rows, Row{519, 5})
	assert(!ok && i == 52)
	i, ok = search(rows, Row{520, 5})
	assert(ok && i == 52)
	i, ok = search(rows, Row{521, 5})
	assert(!ok && i == 53)
	i, ok = search(rows, Row{991, 5})
	assert(!ok && i == len(rows))

	i, ok = search(rows, Row{520, 4})
	assert(!ok && i == 52)
	i, ok = search(rows, Row{520, 6})
	assert(!ok && i == 53)

}

func TestInvalidCompare(t *testing.T) {
	type xtype struct {
		_ [47]byte
	}
	assert(CompareFor[xtype]() == nil)
}

func TestCompareSearchIntFieldsI(t *testing.T) {
	type Row struct {
		a int `btree:"key"`
		b int `btree:"key"`
	}
	_, search := wrapCompareFor[*Row]()
	var rows []*Row
	for i := range 100 {
		rows = append(rows, &Row{i * 10, 5})
	}
	i, ok := search(rows, &Row{-1, 5})
	assert(!ok && i == 0)
	i, ok = search(rows, &Row{519, 5})
	assert(!ok && i == 52)
	i, ok = search(rows, &Row{520, 5})
	assert(ok && i == 52)
	i, ok = search(rows, &Row{521, 5})
	assert(!ok && i == 53)
	i, ok = search(rows, &Row{991, 5})
	assert(!ok && i == len(rows))

	i, ok = search(rows, &Row{520, 4})
	assert(!ok && i == 52)
	i, ok = search(rows, &Row{520, 6})
	assert(!ok && i == 53)

}

func TestNestedCompare(t *testing.T) {

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	type btree struct {
		second int `btree:"key,desc"`
		_      bool
	}
	type ctype struct {
		_     string
		third int `btree:"key"`
	}

	type atype struct {
		first int `btree:"key"`
		btree
		_ int16
		ctype
		_      [47]btree
		fourth int `btree:"key"`
	}

	cmp0 := func(a, b atype) int {
		c := cmp.Compare(a.first, b.first)
		if c == 0 {
			c = cmp.Compare(b.second, a.second)
		}
		if c == 0 {
			c = cmp.Compare(a.third, b.third)
		}
		if c == 0 {
			c = cmp.Compare(a.fourth, b.fourth)
		}
		return c
	}
	cmp1 := CompareFor[atype]()
	for range 1000000 {
		var a, b atype
		a.first = rng.Int() % 2
		a.second = rng.Int() % 2
		a.third = rng.Int() % 2
		a.fourth = rng.Int() % 2
		b.first = rng.Int() % 2
		b.second = rng.Int() % 2
		b.third = rng.Int() % 2
		b.fourth = rng.Int() % 2
		c0 := cmp0(a, b)
		c1 := cmp1(a, b)
		assert(c0 == c1)
	}
}

func TestArrayDrain(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var b0 Array[int]
	a0 := rng.Perm(10000)
	a1 := append([]int{}, a0...)
	slices.Reverse(a1)
	b0.Append(a0...)
	b1 := b0.Copy()
	assert(slices.Equal(a0, slices.Collect(b0.All())))
	assert(slices.Equal(a1, slices.Collect(b0.Backward())))
	assert(b0.Len() == len(a0))
	assert(slices.Equal(a0, slices.Collect(b0.Drain())))
	assert(b0.Len() == 0)
	assert(b1.Len() == len(a0))
	assert(slices.Equal(a1, slices.Collect(b1.DrainBackward())))
	assert(b1.Len() == 0)
}

func TestMapDrain(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var b0 Map[int, omit]
	a0 := rng.Perm(10000)
	for _, key := range a0 {
		b0.Insert(key, omit{})
	}
	slices.Sort(a0)
	a1 := append([]int{}, a0...)
	slices.Reverse(a1)
	b1 := b0.Copy()
	assert(slices.Equal(a0, slices.Collect(iterK(b0.All()))))
	assert(slices.Equal(a1, slices.Collect(iterK(b0.Backward()))))
	assert(b0.Len() == len(a0))
	assert(slices.Equal(a0, slices.Collect(iterK(b0.Drain()))))
	assert(b0.Len() == 0)
	assert(b1.Len() == len(a0))
	assert(slices.Equal(a1, slices.Collect(iterK(b1.DrainBackward()))))
	assert(b1.Len() == 0)
}

func TestSetDrain(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var b0 Set[int]
	a0 := rng.Perm(10000)
	for _, key := range a0 {
		b0.Insert(key)
	}
	slices.Sort(a0)
	a1 := append([]int{}, a0...)
	slices.Reverse(a1)
	b1 := b0.Copy()
	assert(slices.Equal(a0, slices.Collect(b0.All())))
	assert(slices.Equal(a1, slices.Collect(b0.Backward())))
	assert(b0.Len() == len(a0))
	assert(slices.Equal(a0, slices.Collect(b0.Drain())))
	assert(b0.Len() == 0)
	assert(b1.Len() == len(a0))
	assert(slices.Equal(a1, slices.Collect(b1.DrainBackward())))
	assert(b1.Len() == 0)
}

func TestTableDrain(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var b0 Table[int]
	a0 := rng.Perm(10000)
	for _, key := range a0 {
		b0.Insert(key)
	}
	slices.Sort(a0)
	a1 := append([]int{}, a0...)
	slices.Reverse(a1)
	b1 := b0.Copy()
	assert(slices.Equal(a0, slices.Collect(b0.All())))
	assert(slices.Equal(a1, slices.Collect(b0.Backward())))
	assert(b0.Len() == len(a0))
	assert(slices.Equal(a0, slices.Collect(b0.Drain())))
	assert(b0.Len() == 0)
	assert(b1.Len() == len(a0))
	assert(slices.Equal(a1, slices.Collect(b1.DrainBackward())))
	assert(b1.Len() == 0)
}

func TestQueueDrain(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))
	var b0 Queue[int]
	a0 := rng.Perm(10000)
	for _, key := range a0 {
		b0.Push(key)
	}
	assert(slices.Equal(a0, slices.Collect(b0.All())))
	assert(b0.Len() == len(a0))
	assert(slices.Equal(a0, slices.Collect(b0.AllMut())))
	assert(b0.Len() == len(a0))
	assert(slices.Equal(a0, slices.Collect(b0.Drain())))
	assert(b0.Len() == 0)
}

func TestStackDrain(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))
	var b0 Stack[int]
	a0 := rng.Perm(10000)
	for _, key := range a0 {
		b0.Push(key)
	}
	slices.Reverse(a0)
	assert(slices.Equal(a0, slices.Collect(b0.All())))
	assert(b0.Len() == len(a0))
	assert(slices.Equal(a0, slices.Collect(b0.AllMut())))
	assert(b0.Len() == len(a0))
	assert(slices.Equal(a0, slices.Collect(b0.Drain())))
	assert(b0.Len() == 0)
}

func TestDequeDrain(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var b0 Deque[int]
	a0 := rng.Perm(10000)
	a1 := append([]int{}, a0...)
	slices.Reverse(a1)
	for _, key := range a0 {
		b0.PushBack(key)
	}
	b1 := b0.Copy()
	assert(slices.Equal(a0, slices.Collect(b0.All())))
	assert(slices.Equal(a1, slices.Collect(b0.Backward())))
	assert(b0.Len() == len(a0))
	assert(slices.Equal(a0, slices.Collect(b0.Drain())))
	assert(b0.Len() == 0)
	assert(b1.Len() == len(a0))
	assert(slices.Equal(a1, slices.Collect(b1.DrainBackward())))
	assert(b1.Len() == 0)
}

func TestPriqueDrain(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var b0 Prique[int]
	a0 := rng.Perm(10000)
	for _, key := range a0 {
		b0.Push(key)
	}
	slices.Sort(a0)
	a1 := append([]int{}, a0...)
	slices.Reverse(a1)
	assert(slices.Equal(a1, slices.Collect(b0.All())))
	assert(b0.Len() == len(a0))
	assert(slices.Equal(a1, slices.Collect(b0.Drain())))
	assert(b0.Len() == 0)
}

func testPrique(t *testing.T, initopts int) {
	type ttt struct {
		nothing [7]byte
		key     int `btree:"key"`
	}

	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var b Prique[ttt]
	switch initopts {
	case 1:
		b = *NewPrique[ttt]()
	case 2:
		var opts PriqueOptions[ttt]
		opts.Compare = func(a, b ttt) int {
			return cmp.Compare(a.key, b.key)
		}
		opts.Copy = func(a ttt) ttt {
			return a
		}
		opts.Release = func(ttt) {}
		b = *NewPriqueOptions(opts)
	case 3:
		var opts PriqueOptions[ttt]
		opts.Less = func(a, b ttt) bool {
			return a.key < b.key
		}
		opts.Copy = func(a ttt) ttt {
			return a
		}
		opts.Release = func(ttt) {}
		b = *NewPriqueOptions(opts)
	}
	for _, i := range rng.Perm(100) {
		b.Push(ttt{key: i * 10})
	}
	for _, i := range rng.Perm(100) {
		b.Push(ttt{key: i * 10})
	}
	for _, i := range rng.Perm(100) {
		b.Push(ttt{key: i * 10})
	}

	nitems := b.Len()
	assert(nitems == 300)

	var j int
	for i := range 100 {
		for range 3 {
			item, ok := b.At(j)
			assert(ok && item.key == 1000-i*10-10)
			item, ok = b.AtMut(j)
			assert(ok && item.key == 1000-i*10-10)
			j++
		}
	}

	b2 := b.Copy()

	item2, ok := b.Front()
	for item := range b.All() {
		assert(item.key == item2.key && ok)
		break
	}
	first := item2

	for item := range b.AllMut() {
		item2, ok := b.FrontMut()
		assert(item.key == item2.key && ok)
		break
	}

	items := slices.Collect(b.All())
	assert(slices.IsSortedFunc(items, func(a, b ttt) int {
		return cmp.Compare(b.key, a.key)
	}))
	items = slices.Collect(b.AllMut())
	assert(slices.IsSortedFunc(items, func(a, b ttt) int {
		return cmp.Compare(b.key, a.key)
	}))

	item2, ok = b.Front()
	for item := range b.Drain() {
		assert(item.key == item2.key && ok)
		break
	}

	item, ok := b.Front()
	item2, ok2 := b.Pop()
	assert(item.key == item2.key && ok && ok2)

	item, ok = b2.Front()
	assert(item.key == first.key && ok)

	b3 := b2.Copy()

	assert(b2.Len() == nitems)
	item, ok = b2.Delete(ttt{key: 50})
	assert(item.key == 50 && ok)
	item, ok = b2.Delete(ttt{key: 50})
	assert(item.key == 50 && ok)
	item, ok = b2.Delete(ttt{key: 50})
	assert(item.key == 50 && ok)
	item, ok = b2.Delete(ttt{key: 50})
	assert(item.key == 0 && !ok)

	assert(b3.Len() == nitems)
	item, ok = b3.Delete(ttt{key: 50})
	assert(item.key == 50 && ok)
	item, ok = b3.Delete(ttt{key: 50})
	assert(item.key == 50 && ok)
	item, ok = b3.Delete(ttt{key: 50})
	assert(item.key == 50 && ok)
	item, ok = b3.Delete(ttt{key: 50})
	assert(item.key == 0 && !ok)

	assert(b.Len() == nitems-2)
	item, ok = b.Delete(ttt{key: 50})
	assert(item.key == 50 && ok)
	item, ok = b.Delete(ttt{key: 50})
	assert(item.key == 50 && ok)
	item, ok = b.Delete(ttt{key: 50})
	assert(item.key == 50 && ok)
	item, ok = b.Delete(ttt{key: 50})
	assert(item.key == 0 && !ok)

	b.Release()
	assert(b.Len() == 0)
	b2.Release()
	assert(b2.Len() == 0)
	b3.Clear()
	assert(b3.Len() == 0)

}

func TestPrique(t *testing.T) {
	t.Run("opts0", func(t *testing.T) { testPrique(t, 0) })
	t.Run("opts1", func(t *testing.T) { testPrique(t, 1) })
	t.Run("opts2", func(t *testing.T) { testPrique(t, 2) })
	t.Run("opts3", func(t *testing.T) { testPrique(t, 3) })
}

func TestDeque(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	var a []int
	b := NewDeque[int]()
	for i, j := range rng.Perm(100) {
		if i%2 == 0 {
			a = append([]int{j}, a...)
			b.PushFront(j)
		} else {
			a = append(a, j)
			b.PushBack(j)
		}
	}
	a1 := append([]int{}, a...)
	slices.Reverse(a1)

	assert(len(a) == b.Len())

	b2 := b.Copy()

	assert(slices.Equal(a, slices.Collect(b.All())))
	assert(slices.Equal(a, slices.Collect(b.AllMut())))
	assert(slices.Equal(a1, slices.Collect(b.Backward())))
	assert(slices.Equal(a1, slices.Collect(b.BackwardMut())))

	for i := 0; i < len(a); i++ {
		item, ok := b.At(i)
		assert(ok && item == a[i])
		item, ok = b.AtMut(i)
		assert(ok && item == a[i])
	}

	item, ok := b.Front()
	assert(item == a[0] && ok)
	item, ok = b.Back()
	assert(item == a[len(a)-1] && ok)
	item, ok = b.FrontMut()
	assert(item == a[0] && ok)
	item, ok = b.BackMut()
	assert(item == a[len(a)-1] && ok)
	item, ok = b.PopFront()
	assert(item == a[0] && ok)
	item, ok = b.PopBack()
	assert(item == a[len(a)-1] && ok)

	assert(b2.Len() == len(a))
	assert(slices.Equal(a1, slices.Collect(b2.DrainBackward())))
	assert(b2.Len() == 0)
	b2.Release()
	assert(b2.Len() == 0)

	assert(b.Len() == len(a)-2)

	assert(slices.Equal(a[1:len(a)-1], slices.Collect(b.Drain())))
	assert(b.Len() == 0)
	b.Clear()
	assert(b.Len() == 0)

}

func TestTablePushBack(t *testing.T) {
	var b Table[int]

	for i := 0; i < 1000; i++ {
		b.PushBack(i)
	}
	k, _ := b.Back()
	assert(k == 999)
	assert(!b.PushBack(961))
	k, _ = b.Back()
	assert(k == 999)

}

func TestArrayPopIf(t *testing.T) {
	var b Array[int]

	for i := range 100 {
		b.PushBack(i)
	}
	x, ok := b.PopFrontIf(nil)
	assert(ok && x == 0)
	x, ok = b.PopBackIf(nil)
	assert(ok && x == 99)

	x, ok = b.PopFrontIf(func(x int) bool {
		assert(x == 1)
		return false
	})
	assert(!ok && x == 0)
	x, ok = b.PopFrontIf(func(x int) bool {
		assert(x == 1)
		return true
	})
	assert(ok && x == 1)

	x, ok = b.PopBackIf(func(x int) bool {
		assert(x == 98)
		return false
	})
	assert(!ok && x == 0)
	x, ok = b.PopBackIf(func(x int) bool {
		assert(x == 98)
		return true
	})
	assert(ok && x == 98)

}

func TestQueuePopIf(t *testing.T) {
	var b Queue[int]
	for i := range 100 {
		b.Push(i)
	}
	x, ok := b.PopIf(nil)
	assert(ok && x == 0)
	x, ok = b.PopIf(func(x int) bool {
		assert(x == 1)
		return false
	})
	assert(!ok && x == 0)
	x, ok = b.PopIf(func(x int) bool {
		assert(x == 1)
		return true
	})
	assert(ok && x == 1)

}

func TestTablePopIf(t *testing.T) {
	var b Table[int]

	for i := range 100 {
		b.PushBack(i)
	}
	x, ok := b.PopFrontIf(nil)
	assert(ok && x == 0)
	x, ok = b.PopBackIf(nil)
	assert(ok && x == 99)

	x, ok = b.PopFrontIf(func(x int) bool {
		assert(x == 1)
		return false
	})
	assert(!ok && x == 0)
	x, ok = b.PopFrontIf(func(x int) bool {
		assert(x == 1)
		return true
	})
	assert(ok && x == 1)

	x, ok = b.PopBackIf(func(x int) bool {
		assert(x == 98)
		return false
	})
	assert(!ok && x == 0)
	x, ok = b.PopBackIf(func(x int) bool {
		assert(x == 98)
		return true
	})
	assert(ok && x == 98)

}

func TestDequePopIf(t *testing.T) {
	var b Deque[int]

	for i := range 100 {
		b.PushBack(i)
	}
	x, ok := b.PopFrontIf(nil)
	assert(ok && x == 0)
	x, ok = b.PopBackIf(nil)
	assert(ok && x == 99)

	x, ok = b.PopFrontIf(func(x int) bool {
		assert(x == 1)
		return false
	})
	assert(!ok && x == 0)
	x, ok = b.PopFrontIf(func(x int) bool {
		assert(x == 1)
		return true
	})
	assert(ok && x == 1)

	x, ok = b.PopBackIf(func(x int) bool {
		assert(x == 98)
		return false
	})
	assert(!ok && x == 0)
	x, ok = b.PopBackIf(func(x int) bool {
		assert(x == 98)
		return true
	})
	assert(ok && x == 98)

}

func TestPriquePopIf(t *testing.T) {
	var b Prique[int]
	for i := range 100 {
		b.Push(i)
	}
	x, ok := b.PopIf(nil)
	assert(ok && x == 99)
	x, ok = b.PopIf(func(x int) bool {
		assert(x == 98)
		return false
	})
	assert(!ok && x == 0)
	x, ok = b.PopIf(func(x int) bool {
		assert(x == 98)
		return true
	})
	assert(ok && x == 98)

}

func TestMapPopIf(t *testing.T) {
	N := 10000
	var c Map[int, int]
	isempty := func(k, v int) bool {
		return k == 0 && v == 0
	}
	c.tree.Sane(isempty)

	key, value, deleted := c.PopFrontIf(nil)
	assert(!deleted && key == 0 && value == 0)

	key, value, ok := c.Front()
	assert(!ok && key == 0 && value == 0)

	key, value, deleted = c.PopBackIf(nil)
	assert(!deleted && key == 0 && value == 0)

	key, value, ok = c.Back()
	assert(!ok && key == 0 && value == 0)

	for i := range N {
		key := N - i - 1
		ok := c.PushFront(key, -key)
		assert(ok)
		c.tree.Sane(isempty)

		ok = c.PushFront(key, -key)
		assert(!ok)
		assert(c.Len() == i+1)

		k, v, ok := c.Front()
		assert(ok && k == key && v == -k)

		k, v, ok = c.Back()
		assert(ok && k == N-1 && v == -k)
	}
	c.tree.Sane(isempty)
	for i := range N {
		key, value, deleted := c.PopFrontIf(nil)
		assert(deleted && key == i && value == -key)
		assert(c.Len() == N-i-1)
		c.PushFront(key, value)
		key, value, deleted = c.PopFrontIf(func(key int, value int) bool {
			assert(key == i && value == -key)
			return false
		})
		assert(key == 0 && value == 0 && !deleted)
		key, value, deleted = c.PopFrontIf(func(key int, value int) bool {
			assert(key == i && value == -key)
			return true
		})
		assert(deleted && key == i && value == -key)
		assert(c.Len() == N-i-1)

		c.tree.Sane(isempty)
	}
	c.tree.Sane(isempty)
	assert(c.Len() == 0)

	for i := range N {
		key := i

		ok := c.PushBack(key, -key)
		assert(ok)
		c.tree.Sane(isempty)

		ok = c.PushBack(key, -key)
		assert(!ok)
	}
	c.tree.Sane(isempty)
	for i := range N {
		key, value, ok := c.PopBackIf(nil)
		assert(ok && key == N-i-1 && value == -key)
		c.PushBack(key, value)
		key, value, ok = c.PopBackIf(func(key int, value int) bool {
			assert(key == N-i-1 && value == -key)
			return false
		})
		assert(key == 0 && value == 0 && !ok)

		key, value, ok = c.PopBackIf(func(key int, value int) bool {
			assert(key == N-i-1 && value == -key)
			return true
		})
		assert(ok && key == N-i-1 && value == -key)
		c.tree.Sane(isempty)
	}
	c.tree.Sane(isempty)
	assert(c.Len() == 0)
}

func TestMapGetAction(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	N := 10000
	var c Map[int, int]
	isempty := func(k, v int) bool {
		return k == 0 && v == 0
	}
	c.tree.Sane(isempty)

	for _, i := range rng.Perm(N) {
		c.Insert(i, i)
	}
	c.tree.Sane(isempty)

	for _, i := range rng.Perm(N) {
		c.GetAction(i, func(j int) (int, Action) {
			assert(j == i)
			return j, ActionGet
		})
	}
	c.tree.Sane(isempty)
	for _, i := range rng.Perm(N) {
		if i%2 == 0 {
			c.GetAction(i, func(j int) (int, Action) {
				assert(j == i)
				j *= -1
				return j, ActionReplace
			})
			c.tree.Sane(isempty)
		}
	}
	for _, i := range rng.Perm(N) {
		if i%2 == 0 {
			c.GetAction(i, func(j int) (int, Action) {
				assert(j == -i)
				return j, ActionGet
			})
		}
	}
	for _, i := range rng.Perm(N) {
		if i%2 == 1 {
			c.GetAction(i, func(j int) (int, Action) {
				assert(j == i)
				return j, ActionGet
			})
		}
	}
	var n = c.Len()
	for _, i := range rng.Perm(N) {
		if i%2 == 0 {
			c.GetAction(i, func(j int) (int, Action) {
				assert(j == -i)
				return j, ActionDelete
			})
			c.tree.Sane(isempty)
			n--
			assert(c.Len() == n)
		}
	}

	for _, i := range rng.Perm(N) {
		if i%2 == 0 {
			c.GetAction(i, func(j int) (int, Action) {
				return j, ActionDelete
			})
			c.tree.Sane(isempty)
			assert(c.Len() == n)
		}
	}

	for _, i := range rng.Perm(N) {
		if i%2 == 1 {
			c.GetAction(i, func(j int) (int, Action) {
				assert(j == i)
				return j, ActionDelete
			})
			c.tree.Sane(isempty)
			n--
			assert(c.Len() == n)
		}
	}
	c.tree.Sane(isempty)
	assert(c.Len() == 0)
	c.GetAction(0, func(j int) (int, Action) {
		return j, ActionDelete
	})
	assert(c.Len() == 0)
}
