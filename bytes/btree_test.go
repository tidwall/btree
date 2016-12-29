// Copyright 2014 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package btree

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

func init() {
	seed := time.Now().Unix()
	fmt.Println(seed)
	rand.Seed(seed)
}

// perm returns a random permutation of n Int items in the range [0, n).
func perm(n int) (out []itemT) {
	for _, v := range rand.Perm(n) {
		out = append(out, Int(v))
	}
	return
}

// rang returns an ordered list of Int items in the range [0, n).
func rang(n int) (out []itemT) {
	for i := 0; i < n; i++ {
		out = append(out, Int(i))
	}
	return
}

// all extracts all items from a tree in order as a slice.
func all(t *BTree) (out []itemT) {
	t.Ascend(func(key, value []byte) bool {
		out = append(out, itemT{key, value})
		return true
	})
	return
}

// rangerev returns a reversed ordered list of Int items in the range [0, n).
func rangrev(n int) (out []itemT) {
	for i := n - 1; i >= 0; i-- {
		out = append(out, Int(i))
	}
	return
}

// allrev extracts all items from a tree in reverse order as a slice.
func allrev(t *BTree) (out []itemT) {
	t.Descend(func(key, value []byte) bool {
		out = append(out, itemT{key, value})
		return true
	})
	return
}

var btreeDegree = flag.Int("degree", 32, "B-Tree degree")

func TestBTree(t *testing.T) {
	tr := New(*btreeDegree)
	const treeSize = 10000
	for i := 0; i < 10; i++ {
		if _, min := tr.Min(); min != nil {
			t.Fatalf("empty min, got %+v", min)
		}
		if _, max := tr.Max(); max != nil {
			t.Fatalf("empty max, got %+v", max)
		}
		for _, item := range perm(treeSize) {
			if x := tr.ReplaceOrInsert(item.key, item.value); x != nil {
				t.Fatal("insert found item", item)
			}
		}
		for _, item := range perm(treeSize) {
			if x := tr.ReplaceOrInsert(item.key, item.value); x == nil {
				t.Fatal("insert didn't find item", item)
			}
		}
		_, min := tr.Min()
		if want := Int(0).value; bytes.Compare(min, want) != 0 {
			t.Fatalf("min: want %+v, got %+v", want, min)
		}
		_, max := tr.Max()
		if want := Int(treeSize - 1).value; bytes.Compare(max, want) != 0 {
			t.Fatalf("max: want %+v, got %+v", want, max)
		}
		got := all(tr)
		want := rang(treeSize)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("mismatch:\n got: %v\nwant: %v", got, want)
		}

		gotrev := allrev(tr)
		wantrev := rangrev(treeSize)
		if !reflect.DeepEqual(gotrev, wantrev) {
			t.Fatalf("mismatch:\n got: %v\nwant: %v", got, want)
		}

		for _, item := range perm(treeSize) {
			if x := tr.Delete(item.key); x == nil {
				t.Fatalf("didn't find %v", item)
			}
		}
		if got = all(tr); len(got) > 0 {
			t.Fatalf("some left!: %v", got)
		}
	}
}

func ExampleBTree() {
	tr := New(*btreeDegree)
	for i := 0; i < 10; i++ {
		tr.ReplaceOrInsert(Int(i).kv())
	}
	fmt.Println("len:       ", tr.Len())
	fmt.Println("get3:      ", IntString(tr.Get(Int(3).key)))
	fmt.Println("get100:    ", IntString(tr.Get(Int(100).key)))
	fmt.Println("del4:      ", IntString(tr.Delete(Int(4).key)))
	fmt.Println("del100:    ", IntString(tr.Delete(Int(100).key)))
	fmt.Println("replace5:  ", IntString(tr.ReplaceOrInsert(Int(5).kv())))
	fmt.Println("replace100:", IntString(tr.ReplaceOrInsert(Int(100).kv())))
	fmt.Println("min:       ", IntStringValue(tr.Min()))
	fmt.Println("delmin:    ", IntStringValue(tr.DeleteMin()))
	fmt.Println("max:       ", IntStringValue(tr.Max()))
	fmt.Println("delmax:    ", IntStringValue(tr.DeleteMax()))
	fmt.Println("len:       ", tr.Len())
	// Output:
	// len:        10
	// get3:       3
	// get100:     <nil>
	// del4:       4
	// del100:     <nil>
	// replace5:   5
	// replace100: <nil>
	// min:        0
	// delmin:     0
	// max:        100
	// delmax:     100
	// len:        8
}

func TestDeleteMin(t *testing.T) {
	tr := New(3)
	for _, v := range perm(100) {
		tr.ReplaceOrInsert(v.kv())
	}
	var got []itemT
	for k, v := tr.DeleteMin(); v != nil; k, v = tr.DeleteMin() {
		got = append(got, itemT{k, v})
	}
	if want := rang(100); !reflect.DeepEqual(got, want) {
		t.Fatalf("ascendrange:\n got: %v\nwant: %v", got, want)
	}
}

func TestDeleteMax(t *testing.T) {
	tr := New(3)
	for _, v := range perm(100) {
		tr.ReplaceOrInsert(v.kv())
	}
	var got []itemT
	for k, v := tr.DeleteMax(); v != nil; k, v = tr.DeleteMax() {
		got = append(got, itemT{k, v})
	}
	// Reverse our list.
	for i := 0; i < len(got)/2; i++ {
		got[i], got[len(got)-i-1] = got[len(got)-i-1], got[i]
	}
	if want := rang(100); !reflect.DeepEqual(got, want) {
		t.Fatalf("ascendrange:\n got: %v\nwant: %v", got, want)
	}
}

func TestAscendRange(t *testing.T) {
	tr := New(2)
	for _, v := range perm(100) {
		tr.ReplaceOrInsert(v.kv())
	}
	var got []itemT
	tr.AscendRange(Int(40).key, Int(60).key, func(k, v []byte) bool {
		got = append(got, itemT{k, v})
		return true
	})
	if want := rang(100)[40:60]; !reflect.DeepEqual(got, want) {
		t.Fatalf("ascendrange:\n got: %v\nwant: %v", got, want)
	}
	got = got[:0]
	tr.AscendRange(Int(40).key, Int(60).key, func(k, v []byte) bool {
		if IntInt(k) > 50 {
			return false
		}
		got = append(got, itemT{k, v})
		return true
	})
	if want := rang(100)[40:51]; !reflect.DeepEqual(got, want) {
		t.Fatalf("ascendrange:\n got: %v\nwant: %v", got, want)
	}
}

func TestDescendRange(t *testing.T) {
	tr := New(2)
	for _, v := range perm(100) {
		tr.ReplaceOrInsert(v.kv())
	}
	var got []itemT
	tr.DescendRange(Int(60).key, Int(40).key, func(k, v []byte) bool {
		got = append(got, itemT{k, v})
		return true
	})
	if want := rangrev(100)[39:59]; !reflect.DeepEqual(got, want) {
		t.Fatalf("descendrange:\n got: %v\nwant: %v", got, want)
	}
	got = got[:0]
	tr.DescendRange(Int(60).key, Int(40).key, func(k, v []byte) bool {
		if IntInt(k) < 50 {
			return false
		}
		got = append(got, itemT{k, v})
		return true
	})
	if want := rangrev(100)[39:50]; !reflect.DeepEqual(got, want) {
		t.Fatalf("descendrange:\n got: %v\nwant: %v", got, want)
	}
}
func TestAscendLessThan(t *testing.T) {
	tr := New(*btreeDegree)
	for _, v := range perm(100) {
		tr.ReplaceOrInsert(v.kv())
	}
	var got []itemT
	tr.AscendLessThan(Int(60).key, func(k, v []byte) bool {
		got = append(got, itemT{k, v})
		return true
	})
	if want := rang(100)[:60]; !reflect.DeepEqual(got, want) {
		t.Fatalf("ascendrange:\n got: %v\nwant: %v", got, want)
	}
	got = got[:0]
	tr.AscendLessThan(Int(60).key, func(k, v []byte) bool {
		if IntInt(k) > 50 {
			return false
		}
		got = append(got, itemT{k, v})
		return true
	})
	if want := rang(100)[:51]; !reflect.DeepEqual(got, want) {
		t.Fatalf("ascendrange:\n got: %v\nwant: %v", got, want)
	}
}

func TestDescendLessOrEqual(t *testing.T) {
	tr := New(*btreeDegree)
	for _, v := range perm(100) {
		tr.ReplaceOrInsert(v.kv())
	}
	var got []itemT
	tr.DescendLessOrEqual(Int(40).key, func(k, v []byte) bool {
		got = append(got, itemT{k, v})
		return true
	})
	if want := rangrev(100)[59:]; !reflect.DeepEqual(got, want) {
		t.Fatalf("descendlessorequal:\n got: %v\nwant: %v", got, want)
	}
	got = got[:0]
	tr.DescendLessOrEqual(Int(60).key, func(k, v []byte) bool {
		if IntInt(k) < 50 {
			return false
		}
		got = append(got, itemT{k, v})
		return true
	})
	if want := rangrev(100)[39:50]; !reflect.DeepEqual(got, want) {
		t.Fatalf("descendlessorequal:\n got: %v\nwant: %v", got, want)
	}
}
func TestAscendGreaterOrEqual(t *testing.T) {
	tr := New(*btreeDegree)
	for _, v := range perm(100) {
		tr.ReplaceOrInsert(v.kv())
	}
	var got []itemT
	tr.AscendGreaterOrEqual(Int(40).key, func(k, v []byte) bool {
		got = append(got, itemT{k, v})
		return true
	})
	if want := rang(100)[40:]; !reflect.DeepEqual(got, want) {
		t.Fatalf("ascendrange:\n got: %v\nwant: %v", got, want)
	}
	got = got[:0]
	tr.AscendGreaterOrEqual(Int(40).key, func(k, v []byte) bool {
		if IntInt(k) > 50 {
			return false
		}
		got = append(got, itemT{k, v})
		return true
	})
	if want := rang(100)[40:51]; !reflect.DeepEqual(got, want) {
		t.Fatalf("ascendrange:\n got: %v\nwant: %v", got, want)
	}
}

func TestDescendGreaterThan(t *testing.T) {
	tr := New(*btreeDegree)
	for _, v := range perm(100) {
		tr.ReplaceOrInsert(v.kv())
	}
	var got []itemT
	tr.DescendGreaterThan(Int(40).key, func(k, v []byte) bool {
		got = append(got, itemT{k, v})
		return true
	})
	if want := rangrev(100)[:59]; !reflect.DeepEqual(got, want) {
		t.Fatalf("descendgreaterthan:\n got: %v\nwant: %v", got, want)
	}
	got = got[:0]
	tr.DescendGreaterThan(Int(40).key, func(k, v []byte) bool {
		if IntInt(k) < 50 {
			return false
		}
		got = append(got, itemT{k, v})
		return true
	})
	if want := rangrev(100)[:50]; !reflect.DeepEqual(got, want) {
		t.Fatalf("descendgreaterthan:\n got: %v\nwant: %v", got, want)
	}
}

const benchmarkTreeSize = 10000

func BenchmarkInsert(b *testing.B) {
	b.StopTimer()
	insertP := perm(benchmarkTreeSize)
	b.StartTimer()
	i := 0
	for i < b.N {
		tr := New(*btreeDegree)
		for _, item := range insertP {
			tr.ReplaceOrInsert(item.kv())
			i++
			if i >= b.N {
				return
			}
		}
	}
}

func BenchmarkDeleteInsert(b *testing.B) {
	b.StopTimer()
	insertP := perm(benchmarkTreeSize)
	tr := New(*btreeDegree)
	for _, item := range insertP {
		tr.ReplaceOrInsert(item.kv())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tr.Delete(insertP[i%benchmarkTreeSize].key)
		tr.ReplaceOrInsert(insertP[i%benchmarkTreeSize].kv())
	}
}

func BenchmarkDeleteInsertCloneOnce(b *testing.B) {
	b.StopTimer()
	insertP := perm(benchmarkTreeSize)
	tr := New(*btreeDegree)
	for _, item := range insertP {
		tr.ReplaceOrInsert(item.kv())
	}
	tr = tr.Clone()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tr.Delete(insertP[i%benchmarkTreeSize].key)
		tr.ReplaceOrInsert(insertP[i%benchmarkTreeSize].kv())
	}
}

func BenchmarkDeleteInsertCloneEachTime(b *testing.B) {
	b.StopTimer()
	insertP := perm(benchmarkTreeSize)
	tr := New(*btreeDegree)
	for _, item := range insertP {
		tr.ReplaceOrInsert(item.kv())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tr = tr.Clone()
		tr.Delete(insertP[i%benchmarkTreeSize].key)
		tr.ReplaceOrInsert(insertP[i%benchmarkTreeSize].kv())
	}
}

func BenchmarkDelete(b *testing.B) {
	b.StopTimer()
	insertP := perm(benchmarkTreeSize)
	removeP := perm(benchmarkTreeSize)
	b.StartTimer()
	i := 0
	for i < b.N {
		b.StopTimer()
		tr := New(*btreeDegree)
		for _, v := range insertP {
			tr.ReplaceOrInsert(v.kv())
		}
		b.StartTimer()
		for _, item := range removeP {
			tr.Delete(item.key)
			i++
			if i >= b.N {
				return
			}
		}
		if tr.Len() > 0 {
			panic(tr.Len())
		}
	}
}

func BenchmarkGet(b *testing.B) {
	b.StopTimer()
	insertP := perm(benchmarkTreeSize)
	removeP := perm(benchmarkTreeSize)
	b.StartTimer()
	i := 0
	for i < b.N {
		b.StopTimer()
		tr := New(*btreeDegree)
		for _, v := range insertP {
			tr.ReplaceOrInsert(v.kv())
		}
		b.StartTimer()
		for _, item := range removeP {
			tr.Get(item.key)
			i++
			if i >= b.N {
				return
			}
		}
	}
}

func BenchmarkGetCloneEachTime(b *testing.B) {
	b.StopTimer()
	insertP := perm(benchmarkTreeSize)
	removeP := perm(benchmarkTreeSize)
	b.StartTimer()
	i := 0
	for i < b.N {
		b.StopTimer()
		tr := New(*btreeDegree)
		for _, v := range insertP {
			tr.ReplaceOrInsert(v.kv())
		}
		b.StartTimer()
		for _, item := range removeP {
			tr = tr.Clone()
			tr.Get(item.key)
			i++
			if i >= b.N {
				return
			}
		}
	}
}

type byInts []itemT

func (a byInts) Len() int {
	return len(a)
}

func (a byInts) Less(i, j int) bool {
	return bytes.Compare(a[i].key, a[j].key) < 0
}

func (a byInts) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func BenchmarkAscend(b *testing.B) {
	arr := perm(benchmarkTreeSize)
	tr := New(*btreeDegree)
	for _, v := range arr {
		tr.ReplaceOrInsert(v.kv())
	}
	sort.Sort(byInts(arr))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := 0
		tr.Ascend(func(k, v []byte) bool {
			if IntInt(k) != IntInt(arr[j].key) {
				b.Fatalf("mismatch: expected: %v, got %v", IntInt(arr[j].key), IntInt(k))
			}
			j++
			return true
		})
	}
}

func BenchmarkDescend(b *testing.B) {
	arr := perm(benchmarkTreeSize)
	tr := New(*btreeDegree)
	for _, v := range arr {
		tr.ReplaceOrInsert(v.kv())
	}
	sort.Sort(byInts(arr))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := len(arr) - 1
		tr.Descend(func(k, v []byte) bool {
			if IntInt(k) != IntInt(arr[j].key) {
				b.Fatalf("mismatch: expected: %v, got %v", IntInt(arr[j].key), IntInt(k))
			}
			j--
			return true
		})
	}
}
func BenchmarkAscendRange(b *testing.B) {
	arr := perm(benchmarkTreeSize)
	tr := New(*btreeDegree)
	for _, v := range arr {
		tr.ReplaceOrInsert(v.kv())
	}
	sort.Sort(byInts(arr))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := 100
		tr.AscendRange(Int(100).key, arr[len(arr)-100].key, func(k, v []byte) bool {
			if IntInt(k) != IntInt(arr[j].key) {
				b.Fatalf("mismatch: expected: %v, got %v", IntInt(arr[j].key), IntInt(k))
			}
			j++
			return true
		})
		if j != len(arr)-100 {
			b.Fatalf("expected: %v, got %v", len(arr)-100, j)
		}
	}
}

func BenchmarkDescendRange(b *testing.B) {
	arr := perm(benchmarkTreeSize)
	tr := New(*btreeDegree)
	for _, v := range arr {
		tr.ReplaceOrInsert(v.kv())
	}
	sort.Sort(byInts(arr))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := len(arr) - 100
		tr.DescendRange(arr[len(arr)-100].key, Int(100).key, func(k, v []byte) bool {
			if IntInt(k) != IntInt(arr[j].key) {
				b.Fatalf("mismatch: expected: %v, got %v", IntInt(arr[j].key), IntInt(k))
			}
			j--
			return true
		})
		if j != 100 {
			b.Fatalf("expected: %v, got %v", len(arr)-100, j)
		}
	}
}
func BenchmarkAscendGreaterOrEqual(b *testing.B) {
	arr := perm(benchmarkTreeSize)
	tr := New(*btreeDegree)
	for _, v := range arr {
		tr.ReplaceOrInsert(v.kv())
	}
	sort.Sort(byInts(arr))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := 100
		kk := 0
		tr.AscendGreaterOrEqual(Int(100).key, func(k, v []byte) bool {
			if IntInt(k) != IntInt(arr[j].key) {
				b.Fatalf("mismatch: expected: %v, got %v", IntInt(arr[j].key), IntInt(k))
			}
			j++
			kk++
			return true
		})
		if j != len(arr) {
			b.Fatalf("expected: %v, got %v", len(arr), j)
		}
		if kk != len(arr)-100 {
			b.Fatalf("expected: %v, got %v", len(arr)-100, kk)
		}
	}
}
func BenchmarkDescendLessOrEqual(b *testing.B) {
	arr := perm(benchmarkTreeSize)
	tr := New(*btreeDegree)
	for _, v := range arr {
		tr.ReplaceOrInsert(v.kv())
	}
	sort.Sort(byInts(arr))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := len(arr) - 100
		kk := len(arr)
		tr.DescendLessOrEqual(arr[len(arr)-100].key, func(k, v []byte) bool {
			if IntInt(k) != IntInt(arr[j].key) {
				b.Fatalf("mismatch: expected: %v, got %v", IntInt(arr[j].key), IntInt(k))
			}
			j--
			kk--
			return true
		})
		if j != -1 {
			b.Fatalf("expected: %v, got %v", -1, j)
		}
		if kk != 99 {
			b.Fatalf("expected: %v, got %v", 99, kk)
		}
	}
}

const cloneTestSize = 10000

func cloneTest(t *testing.T, b *BTree, start int, p []itemT, wg *sync.WaitGroup, trees *[]*BTree) {
	t.Logf("Starting new clone at %v", start)
	*trees = append(*trees, b)
	for i := start; i < cloneTestSize; i++ {
		b.ReplaceOrInsert(p[i].kv())
		if i%(cloneTestSize/5) == 0 {
			wg.Add(1)
			go cloneTest(t, b.Clone(), i+1, p, wg, trees)
		}
	}
	wg.Done()
}

func TestCloneConcurrentOperations(t *testing.T) {
	b := New(*btreeDegree)
	trees := []*BTree{}
	p := perm(cloneTestSize)
	var wg sync.WaitGroup
	wg.Add(1)
	go cloneTest(t, b, 0, p, &wg, &trees)
	wg.Wait()
	want := rang(cloneTestSize)
	t.Logf("Starting equality checks on %d trees", len(trees))
	for i, tree := range trees {
		if !reflect.DeepEqual(want, all(tree)) {
			t.Errorf("tree %v mismatch", i)
		}
	}
	t.Log("Removing half from first half")
	toRemove := rang(cloneTestSize)[cloneTestSize/2:]
	for i := 0; i < len(trees)/2; i++ {
		tree := trees[i]
		wg.Add(1)
		go func() {
			for _, item := range toRemove {
				tree.Delete(item.key)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	t.Log("Checking all values again")
	for i, tree := range trees {
		var wantpart []itemT
		if i < len(trees)/2 {
			wantpart = want[:cloneTestSize/2]
		} else {
			wantpart = want
		}
		if got := all(tree); !reflect.DeepEqual(wantpart, got) {
			t.Errorf("tree %v mismatch, want %v got %v", i, len(want), len(got))
		}
	}
}

// Int implements the Item interface for integers.
func Int(i int) itemT {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return itemT{key: b, value: b}
}
func IntInt(b []byte) int {
	return int(binary.BigEndian.Uint64(b))
}
func (item itemT) kv() ([]byte, []byte) {
	return item.key, item.value
}
func IntString(b []byte) string {
	if b == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d", int(binary.BigEndian.Uint64(b)))
}
func IntStringValue(key, b []byte) string {
	if b == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d", int(binary.BigEndian.Uint64(b)))
}
