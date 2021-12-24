package btree

import (
	"testing"
)

func TestSet(t *testing.T) {
	N := 1_000_000
	var tr Set[int]
	for i := 0; i < N; i++ {
		tr.Load(i)
	}
	assert(tr.Len() == N)
	for i := 0; i < N; i++ {
		assert(tr.Contains(i))
	}

	count := 0
	tr.Scan(func(_ int) bool {
		count++
		return true
	})
	assert(count == N)
	count = 0
	tr.Ascend(N/2, func(_ int) bool {
		count++
		return true
	})
	assert(count == N/2)

	count = 0
	tr.Reverse(func(_ int) bool {
		count++
		return true
	})
	assert(count == N)
	count = 0
	tr.Descend(N/2, func(_ int) bool {
		count++
		return true
	})
	assert(count == N/2+1)

	for i := 0; i < N; i++ {
		tr.Delete(i)
	}

	dotup := func(v int, ok bool) interface{} {
		if !ok {
			return nil
		}
		return v
	}

	assert(tr.Len() == 0)
	assert(dotup(tr.Min()) == nil)
	assert(dotup(tr.Max()) == nil)
	assert(dotup(tr.PopMin()) == nil)
	assert(dotup(tr.PopMax()) == nil)
	for i := 0; i < N; i++ {
		assert(!tr.Contains(i))
	}
	for i := 0; i < N; i++ {
		tr.Insert(i)
	}
	assert(tr.Len() == N)
	for i := 0; i < N; i++ {
		tr.Insert(i)
	}
	assert(tr.Len() == N)
	for i := 0; i < N; i++ {
		tr.Load(i)
	}
	assert(tr.Len() == N)
	assert(dotup(tr.Min()) == 0)
	assert(dotup(tr.Max()) == N-1)
	assert(dotup(tr.PopMin()) == 0)
	assert(dotup(tr.PopMax()) == N-1)
	tr.Insert(0)
	tr.Insert(N - 1)
	assert(dotup(tr.GetAt(0)) == 0)
	assert(dotup(tr.GetAt(N)) == nil)
	tr.Insert(N - 1)
	assert(tr.Height() > 0)
	assert(dotup(tr.DeleteAt(0)) == 0)
	tr.Insert(0)
	assert(dotup(tr.DeleteAt(N-1)) == N-1)
	assert(dotup(tr.DeleteAt(N)) == nil)
	tr.Insert(N - 1)

	count = 0
	tr.Scan(func(item int) bool {
		count++
		return true
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
	for i := 0; i < N; i++ {
		assert(tr.Contains(i))
	}
	for i := 0; i < N; i++ {
		tr.Delete(i)
	}
	for i := 0; i < N; i++ {
		assert(!tr.Contains(i))
	}
	assert(tr.base.less(1, 2))
	assert(tr.base.less(2, 10))
}

func TestSetIter(t *testing.T) {
	N := 100_000
	lt := func(a, b int) bool { return a < b }
	eq := func(a, b int) bool { return !lt(a, b) && !lt(b, a) }
	var tr Set[int]
	var all []int
	for i := 0; i < N; i++ {
		tr.Load(i)
		all = append(all, i)
	}
	var count int
	var i int
	iter := tr.Iter()
	for ok := iter.First(); ok; ok = iter.Next() {
		if !eq(all[i], iter.Key()) {
			panic("!")
		}
		count++
		i++
	}
	if count != N {
		t.Fatalf("expected %v, got %v", N, count)
	}

	count = 0
	i = len(all) - 1
	iter = tr.Iter()
	for ok := iter.Last(); ok; ok = iter.Prev() {
		if !eq(all[i], iter.Key()) {
			panic("!")
		}
		i--
		count++
	}
	if count != N {
		t.Fatalf("expected %v, got %v", N, count)
	}

	i = 0
	iter = tr.Iter()
	for ok := iter.First(); ok; ok = iter.Next() {
		if !eq(all[i], iter.Key()) {
			panic("!")
		}
		i++
	}
	i--
	for ok := iter.Prev(); ok; ok = iter.Prev() {
		i--
		if !eq(all[i], iter.Key()) {
			panic("!")
		}

	}
	if i != 0 {
		panic("!")
	}

	i++
	for ok := iter.Next(); ok; ok = iter.Next() {
		if !eq(all[i], iter.Key()) {
			panic("!")
		}
		i++

	}
	if i != N {
		panic("!")
	}

	i = 0
	for ok := iter.First(); ok; ok = iter.Next() {
		if !eq(all[i], iter.Key()) {
			panic("!")
		}
		if eq(iter.Key(), N/2) {
			for ok = iter.Prev(); ok; ok = iter.Prev() {
				i--
				if !eq(all[i], iter.Key()) {
					panic("!")
				}
			}
			break
		}
		i++
	}
}
