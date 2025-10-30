package btree

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

func assert(x bool) {
	if !x {
		panic("assert failed")
	}
}

func intLess(a, b interface{}) bool {
	return a.(int) < b.(int)
}

func TestBTree(t *testing.T) {
	func() {
		defer func() {
			msg, ok := recover().(string)
			assert(ok && msg == "nil less")
		}()
		New(nil)
	}()
	func() {
		defer func() {
			msg, ok := recover().(string)
			assert(ok && msg == "nil less")
		}()
		NewNonConcurrent(nil)
	}()
	N := 1_000_000
	for j := 0; j < 2; j++ {
		var tr *BTree
		if j == 0 {
			tr = New(intLess)
		} else {
			tr = NewNonConcurrent(intLess)
		}
		for i := 0; i < N; i++ {
			assert(tr.Load(i) == nil)
		}
		assert(tr.Len() == N)
		for i := 0; i < N; i++ {
			assert(tr.Get(i) == i)
		}

		count := 0
		tr.Ascend(nil, func(_ interface{}) bool {
			count++
			return true
		})
		assert(count == N)
		count = 0
		tr.Ascend(N/2, func(_ interface{}) bool {
			count++
			return true
		})
		assert(count == N/2)

		count = 0
		tr.Descend(nil, func(_ interface{}) bool {
			count++
			return true
		})
		assert(count == N)
		count = 0
		tr.Descend(N/2, func(_ interface{}) bool {
			count++
			return true
		})
		assert(count == N/2+1)

		for i := 0; i < N; i++ {
			assert(tr.Delete(i) == i)
		}
		assert(tr.Len() == 0)
		assert(tr.Min() == nil)
		assert(tr.Max() == nil)
		assert(tr.PopMin() == nil)
		assert(tr.PopMax() == nil)

		for i := 0; i < N; i++ {
			assert(tr.Get(i) == nil)
		}
		for i := 0; i < N; i++ {
			assert(tr.Set(i) == nil)
		}
		assert(tr.Len() == N)
		var hint PathHint
		for i := 0; i < N; i++ {
			assert(tr.SetHint(i, &hint) == i)
		}
		assert(tr.Len() == N)
		for i := 0; i < N; i++ {
			assert(tr.Load(i) == i)
		}
		assert(tr.Len() == N)
		assert(tr.Min() == 0)
		assert(tr.Max() == N-1)
		assert(tr.PopMin() == 0)
		assert(tr.PopMax() == N-1)
		assert(tr.Set(0) == nil)
		assert(tr.Set(N-1) == nil)
		assert(tr.GetAt(0) == 0)
		assert(tr.GetAt(N) == nil)
		assert(tr.Set(N-1) == N-1)
		assert(tr.Height() > 0)
		assert(tr.DeleteAt(0) == 0)
		assert(tr.Set(0) == nil)
		assert(tr.DeleteAt(N-1) == N-1)
		assert(tr.DeleteAt(N) == nil)
		var wg sync.WaitGroup
		wg.Add(1)
		go func(tr *BTree) {
			wg.Wait()
			count := 0
			tr.Walk(func(items []interface{}) {
				count += len(items)
			})
			assert(count == N-1)
		}(tr.Copy())
		for i := 0; i < N/2; i++ {
			tr.Delete(i)
		}
		for i := 0; i < N; i++ {
			tr.Set(i)
		}
		wg.Done()

		count = 0
		tr.Walk(func(items []interface{}) {
			count += len(items)
		})
		assert(count == N)

		func() {
			defer func() {
				msg, ok := recover().(string)
				assert(ok && msg == "nil item")
			}()
			tr := NewNonConcurrent(intLess)
			tr.Set(nil)
		}()
		func() {
			defer func() {
				msg, ok := recover().(string)
				assert(ok && msg == "nil item")
			}()
			tr := NewNonConcurrent(intLess)
			tr.Load(nil)
		}()
		assert(tr.Get(nil) == nil)
		assert(tr.Delete(nil) == nil)
		for i := 0; i < N; i++ {
			assert(tr.GetHint(i, &hint) == i)
		}
		for i := 0; i < N; i++ {
			assert(tr.DeleteHint(i, &hint) == i)
		}
		for i := 0; i < N; i++ {
			assert(tr.GetHint(i, &hint) == nil)
		}
		for i := 0; i < N; i++ {
			assert(tr.DeleteHint(i, &hint) == nil)
		}

		assert(tr.Less(1, 2))
		assert(tr.Less(2, 10))
	}
}

func TestClear(t *testing.T) {
	tr := New(intLess)
	for i := 0; i < 100; i++ {
		tr.Set(i)
	}
	assert(tr.Len() == 100)
	tr.Clear()
	assert(tr.Len() == 0)
	for i := 0; i < 100; i++ {
		tr.Set(i)
	}
	assert(tr.Len() == 100)
}

func TestIter(t *testing.T) {
	N := 100_000
	lt := func(a, b interface{}) bool { return a.(int) < b.(int) }
	eq := func(a, b interface{}) bool { return !lt(a, b) && !lt(b, a) }
	tr := New(lt)
	var all []int
	for i := 0; i < N; i++ {
		tr.Load(i)
		all = append(all, i)
	}
	var count int
	var i int
	iter := tr.Iter()
	for ok := iter.First(); ok; ok = iter.Next() {
		if !eq(all[i], iter.Item()) {
			panic("!")
		}
		count++
		i++
	}
	if count != N {
		t.Fatalf("expected %v, got %v", N, count)
	}
	iter.Release()
	count = 0
	i = len(all) - 1
	iter = tr.Iter()
	for ok := iter.Last(); ok; ok = iter.Prev() {
		if !eq(all[i], iter.Item()) {
			panic("!")
		}
		i--
		count++
	}
	if count != N {
		t.Fatalf("expected %v, got %v", N, count)
	}
	iter.Release()
	i = 0
	iter = tr.Iter()
	for ok := iter.First(); ok; ok = iter.Next() {
		if !eq(all[i], iter.Item()) {
			panic("!")
		}
		i++
	}
	i--
	for ok := iter.Prev(); ok; ok = iter.Prev() {
		i--
		if !eq(all[i], iter.Item()) {
			panic("!")
		}

	}
	if i != 0 {
		panic("!")
	}

	i++
	for ok := iter.Next(); ok; ok = iter.Next() {
		if !eq(all[i], iter.Item()) {
			panic("!")
		}
		i++

	}
	if i != N {
		panic("!")
	}

	i = 0
	for ok := iter.First(); ok; ok = iter.Next() {
		if !eq(all[i], iter.Item()) {
			panic("!")
		}
		if eq(iter.Item(), N/2) {
			for ok = iter.Prev(); ok; ok = iter.Prev() {
				i--
				if !eq(all[i], iter.Item()) {
					panic("!")
				}
			}
			break
		}
		i++
	}
	iter.Release()
}

type largeItem struct {
	a uint64
	b uint64
	c uint64
	d uint64
}

func useIterator(iter IterG[largeItem]) {
	iter.Seek(largeItem{a: 0})
	defer iter.ReleaseReuseable()

	// Iterate over 10 items beginning the seeked item.
	assert(iter.Item().a == 0)
}

func useIteratorPointer(iter *IterG[largeItem]) {
	iter.Seek(largeItem{a: 0})
	defer iter.ReleaseReuseable()

	assert(iter.Item().a == 0)
}

// This benchmark proves that there exist cases where the iterator creation can
// cause an allocation
//
// Run using: go test -run=^$ -bench ^BenchmarkIteratorCreationAlloc$ github.com/tidwall/btree
func BenchmarkIteratorCreationAlloc(b *testing.B) {
	tr := NewBTreeG(func(a, b largeItem) bool {
		return a.a < b.a
	})

	for i := 0; i < 1; i++ {
		tr.Set(largeItem{a: uint64(i * 2), b: uint64(i * 2), c: uint64(i * 2), d: uint64(i * 2)})
	}

	iter := tr.Iter()
	iter.Seek(largeItem{a: 0})
	assert(iter.Item().a == 0)

	// The following will cause 1 allocation per op. Note that this allocation is not due
	// to iter.stack slice allocating to grow larger. The benchmark is only performing a
	// single seek, and the btree only has 1 item. The allocation is due to the iterator
	// escaping to the heap.
	b.Run("no reuse", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			iter := tr.Iter()
			useIterator(iter)
		}
	})

	reusableIter := tr.Iter()
	reusableIterPointer := &reusableIter

	// The following will cause 0 allocations per op, since a re-usable iterator is used.
	b.Run("reuse", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			reusableIterPointer.Init(tr)
			useIteratorPointer(reusableIterPointer)
		}
	})
}

// Ensure that the re-usable iterator works as expected even if the tree
// is receiving new writes after every iteration.
func TestBenchmarkIteratorReuseWorks(t *testing.T) {
	tr := NewBTreeGOptions(func(a, b largeItem) bool {
		return a.a < b.a
	}, Options{
		NoLocks: true,
	})

	tr.Set(largeItem{a: 0, b: 0, c: 0, d: 0})
	iter := tr.Iter()
	reusableIter := &iter

	found := reusableIter.Seek(largeItem{a: 0})
	assert(found)
	assert(reusableIter.Item().a == 0)
	reusableIter.ReleaseReuseable()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 1; i < 10000; i++ {
		// Insert a single item. Reset iterator, randomly seek b/w 0 to i, and ensure that the
		// iterator is seeing the expected items.
		tr.Set(largeItem{a: uint64(i), b: uint64(i), c: uint64(i), d: uint64(i)})

		seekTo := rng.Intn(i + 1)

		// Reset the iterator.
		reusableIter.Init(tr)

		// Seek to the random position.
		found = reusableIter.Seek(largeItem{a: uint64(seekTo)})
		assert(found)

		nextExpectedItem := uint64(seekTo) + 1
		for iter.Next() {
			assert(iter.Item().a == nextExpectedItem)
			nextExpectedItem++
		}

		assert(nextExpectedItem == uint64(i)+1)

		reusableIter.ReleaseReuseable()
	}
}
