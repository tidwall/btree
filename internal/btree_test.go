package btree

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

var seed int64

func init() {
	var ok bool
	seed, ok = testCustomSeed()
	if !ok {
		var err error
		seed, err = strconv.ParseInt(os.Getenv("SEED"), 10, 64)
		if err != nil {
			seed = time.Now().UnixNano()
		}
	}
	fmt.Printf("seed: %d\n", seed)
	rand.Seed(seed)
}

func randKeys(N int) (keys []kind) {
	keys = make([]kind, N)
	for _, i := range rand.Perm(N) {
		keys[i] = testMakeItem(i)
	}
	return keys
}

// eqtr is used for testing equality
var eqtr = testNewBTree()

func lt(a, b kind) bool  { return eqtr.Less(a, b) }
func eq(a, b kind) bool  { return !(lt(a, b) || lt(b, a)) }
func lte(a, b kind) bool { return lt(a, b) || eq(a, b) }
func gt(a, b kind) bool  { return lt(b, a) }
func gte(a, b kind) bool { return gt(a, b) || eq(a, b) }

func kindsAreEqual(a, b []kind) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if !eq(a[i], b[i]) {
			return false
		}
	}
	return true
}

func TestMakeItemOrder(t *testing.T) {
	ints := []int{0, 1, 2, 3, 4, 10, 20, 30, 40, 100, 200, 300, 400}
	for i := 0; i < len(ints)-1; i++ {
		a := testMakeItem(ints[i])
		b := testMakeItem(ints[i+1])
		if !lt(a, b) {
			t.Fatalf("bad ordering for testMakeItem: '%v' !< '%v'", a, b)
		}
	}
}

func TestDescend(t *testing.T) {
	tr := testNewBTree()
	var count int
	tr.Descend(testMakeItem(rand.Int()), func(item kind) bool {
		count++
		return true
	})
	if count > 0 {
		t.Fatalf("expected 0, got %v", count)
	}
	var keys []kind
	for i := 0; i < 1000; i += 10 {
		keys = append(keys, testMakeItem(i))
		tr.Set(keys[len(keys)-1])
	}
	var exp []kind
	tr.Reverse(func(item kind) bool {
		exp = append(exp, item)
		return true
	})
	for i := 999; i >= 0; i-- {
		key := testMakeItem(i)
		var all []kind
		tr.Descend(key, func(item kind) bool {
			all = append(all, item)
			return true
		})
		for len(exp) > 0 && tr.Less(key, exp[0]) {
			exp = exp[1:]
		}
		var count int
		tr.Descend(key, func(item kind) bool {
			if count == (i+1)%maxItems {
				return false
			}
			count++
			return true
		})
		if count > len(exp) {
			t.Fatalf("expected 1, got %v", count)
		}
		if !kindsAreEqual(exp, all) {
			fmt.Printf("exp: %v\n", exp)
			fmt.Printf("all: %v\n", all)
			t.Fatal("mismatch")
		}
		for j := 0; j < tr.Len(); j++ {
			count = 0
			tr.Descend(key, func(item kind) bool {
				if count == j {
					return false
				}
				count++
				return true
			})
		}
	}
}

func TestAscend(t *testing.T) {
	tr := testNewBTree()
	var count int
	tr.Ascend(testMakeItem(1), func(item kind) bool {
		count++
		return true
	})
	if count > 0 {
		t.Fatalf("expected 0, got %v", count)
	}
	var keys []kind
	for i := 0; i < 1000; i += 10 {
		keys = append(keys, testMakeItem(i))
		tr.Set(keys[len(keys)-1])
		tr.sane()
	}
	exp := keys
	for i := -1; i < 1000; i++ {
		key := testMakeItem(i)
		var all []kind
		tr.Ascend(key, func(item kind) bool {
			all = append(all, item)
			return true
		})
		for len(exp) > 0 && tr.Less(exp[0], key) {
			exp = exp[1:]
		}
		var count int
		tr.Ascend(key, func(item kind) bool {
			if count == (i+1)%maxItems {
				return false
			}
			count++
			return true
		})
		if count > len(exp) {
			t.Fatalf("expected 1, got %v", count)
		}
		if !kindsAreEqual(exp, all) {
			t.Fatal("mismatch")
		}
	}
}

func TestSimpleRandom(t *testing.T) {
	start := time.Now()
	for time.Since(start) < time.Second*2 {
		N := 100_000
		items := randKeys(N)
		tr := testNewBTree()
		tr.sane()
		for i := 0; i < len(items); i++ {
			if v, ok := tr.Get(items[i]); ok || !eq(v, tr.empty) {
				panic("!")
			}
			if v, ok := tr.Set(items[i]); ok || !eq(v, tr.empty) {
				panic("!")
			}
			if v, ok := tr.Get(items[i]); !ok || !eq(v, items[i]) {
				panic("!")
			}
		}
		tr.sane()
		for i := 0; i < len(items); i++ {
			if v, ok := tr.Set(items[i]); !ok || !eq(v, items[i]) {
				panic("!")
			}
		}
		pivot := items[len(items)/2]
		tr.Ascend(pivot, func(item kind) bool {
			if tr.Less(item, pivot) {
				panic("!")
			}
			return true
		})
		var min kind
		index := 0
		tr.Scan(func(item kind) bool {
			if index == len(items)/2 {
				return false
			}
			if index > 0 {
				if tr.Less(item, min) {
					panic("!")
				}
			}
			min = item
			index++
			return true
		})
		tr.sane()
		for i := 0; i < len(items); i++ {
			if v, ok := tr.Delete(items[i]); !ok || !eq(v, items[i]) {
				panic("!")
			}
			if i%97 == 0 {
				tr.sane()
			}
			if v, ok := tr.Delete(items[i]); ok || !eq(v, tr.empty) {
				panic("!")
			}
		}
		if tr.Len() != 0 {
			panic("!")
		}
		tr.sane()
		for i := 0; i < len(items); i++ {
			if v, ok := tr.Delete(items[i]); ok || !eq(v, tr.empty) {
				panic("!")
			}
		}
		tr.sane()
		tr.Scan(func(item kind) bool {
			panic("!")
		})
	}
}

func TestBTree(t *testing.T) {
	N := 10000
	tr := testNewBTree()
	tr.sane()
	keys := randKeys(N)

	// insert all items
	for _, key := range keys {
		if v, ok := tr.Set(key); ok || !eq(v, tr.empty) {
			t.Fatal("expected false")
		}
		tr.sane()
	}

	// check length
	if tr.Len() != len(keys) {
		t.Fatalf("expected %v, got %v", len(keys), tr.Len())
	}

	// get each value
	for _, key := range keys {
		if v, ok := tr.Get(key); !ok || !eq(v, key) {
			t.Fatalf("expected '%v', got '%v'", key, v)
		}
	}

	// scan all items
	var prev kind
	var count int
	tr.Scan(func(item kind) bool {
		if count > 0 {
			if lte(item, prev) {
				t.Fatal("out of order")
			}
		}
		prev = item
		count++
		return true
	})
	if count != len(keys) {
		t.Fatalf("expected '%v', got '%v'", len(keys), count)
	}

	// reverse all items
	count = 0
	tr.Reverse(func(item kind) bool {
		if count > 0 {
			if gte(item, prev) {
				t.Fatal("out of order")
			}
		}
		prev = item
		count++
		return true
	})
	if count != len(keys) {
		t.Fatalf("expected '%v', got '%v'", len(keys), count)
	}

	// try to get an invalid item
	if v, ok := tr.Get(testMakeItem(-1)); ok || !eq(v, tr.empty) {
		t.Fatal("expected nil")
	}

	// scan and quit at various steps
	for i := 0; i < 100; i++ {
		var j int
		tr.Scan(func(item kind) bool {
			if j == i {
				return false
			}
			j++
			return true
		})
	}

	// reverse and quit at various steps
	for i := 0; i < 100; i++ {
		var j int
		tr.Reverse(func(item kind) bool {
			if j == i {
				return false
			}
			j++
			return true
		})
	}

	// delete half the items
	for _, key := range keys[:len(keys)/2] {
		if v, ok := tr.Delete(key); !ok || !eq(v, key) {
			t.Fatalf("expected '%v', got '%v'", key, v)
		}
	}

	// check length
	if tr.Len() != len(keys)/2 {
		t.Fatalf("expected %v, got %v", len(keys)/2, tr.Len())
	}

	// try delete half again
	for _, key := range keys[:len(keys)/2] {
		if v, ok := tr.Delete(key); ok || !eq(v, tr.empty) {
			t.Fatal("expected false")
		}
		tr.sane()
	}

	// check length
	if tr.Len() != len(keys)/2 {
		t.Fatalf("expected %v, got %v", len(keys)/2, tr.Len())
	}

	// scan items
	count = 0
	tr.Scan(func(item kind) bool {
		if count > 0 {
			if lte(item, prev) {
				t.Fatal("out of order")
			}
		}
		prev = item
		count++
		return true
	})
	if count != len(keys)/2 {
		t.Fatalf("expected '%v', got '%v'", len(keys), count)
	}

	// replace second half
	for _, key := range keys[len(keys)/2:] {
		if v, ok := tr.Set(key); !ok || !eq(v, key) {
			t.Fatalf("expected '%v', got '%v'", key, v)
		}
		tr.sane()
	}

	// delete next half the items
	for _, key := range keys[len(keys)/2:] {
		if v, ok := tr.Delete(key); !ok || !eq(v, key) {
			t.Fatalf("expected '%v', got '%v'", key, v)
		}
		tr.sane()
	}

	// check length
	if tr.Len() != 0 {
		t.Fatalf("expected %v, got %v", 0, tr.Len())
	}

	// do some stuff on an empty tree
	if v, ok := tr.Get(keys[0]); ok || !eq(v, tr.empty) {
		t.Fatal("expected nil")
	}
	tr.Scan(func(item kind) bool {
		t.Fatal("should not be reached")
		return true
	})
	tr.Reverse(func(item kind) bool {
		t.Fatal("should not be reached")
		return true
	})
	if v, ok := tr.Delete(testMakeItem(-1)); ok || !eq(v, tr.empty) {
		t.Fatal("expected nil")
	}
	tr.sane()
}

func TestBTreeOne(t *testing.T) {
	tr := testNewBTree()
	tr.Set(testMakeItem(1))
	tr.Delete(testMakeItem(1))
	tr.Set(testMakeItem(1))
	tr.Delete(testMakeItem(1))
	tr.Set(testMakeItem(1))
	tr.Delete(testMakeItem(1))
	if tr.Len() != 0 {
		panic("!")
	}
	tr.sane()
}

func TestBTree256(t *testing.T) {
	tr := testNewBTree()
	var n int
	for j := 0; j < 2; j++ {
		for _, i := range rand.Perm(256) {
			tr.Set(testMakeItem(i))
			n++
			if tr.Len() != n {
				t.Fatalf("expected 256, got %d", n)
			}
		}
		for _, i := range rand.Perm(256) {
			if v, ok := tr.Get(testMakeItem(i)); !ok || !eq(v, testMakeItem(i)) {
				t.Fatalf("expected %v, got %v", i, v)
			}
		}
		for _, i := range rand.Perm(256) {
			tr.Delete(testMakeItem(i))
			n--
			if tr.Len() != n {
				t.Fatalf("expected 256, got %d", n)
			}
		}
		for _, i := range rand.Perm(256) {
			if v, ok := tr.Get(testMakeItem(i)); ok || !eq(v, tr.empty) {
				t.Fatal("expected nil")
			}
		}
	}
}

func shuffleItems(keys []kind) {
	for i := range keys {
		j := rand.Intn(i + 1)
		keys[i], keys[j] = keys[j], keys[i]
	}
}

func sortItems(keys []kind) {
	sort.Slice(keys, func(i, j int) bool {
		return lt(keys[i], keys[j])
	})
}

func TestRandom(t *testing.T) {
	N := 200000
	keys := randKeys(N)
	tr := testNewBTree()
	tr.sane()
	if v, ok := tr.Min(); ok || !eq(v, tr.empty) {
		t.Fatalf("expected nil")
	}
	if v, ok := tr.Max(); ok || !eq(v, tr.empty) {
		t.Fatalf("expected nil")
	}
	if v, ok := tr.PopMin(); ok || !eq(v, tr.empty) {
		t.Fatalf("expected nil")
	}
	if v, ok := tr.PopMax(); ok || !eq(v, tr.empty) {
		t.Fatalf("expected nil")
	}
	if tr.Height() != 0 {
		t.Fatalf("expected 0, got %d", tr.Height())
	}
	tr.sane()
	shuffleItems(keys)
	for i := 0; i < len(keys); i++ {
		if v, ok := tr.Set(keys[i]); ok || !eq(v, tr.empty) {
			t.Fatalf("expected nil")
		}
		if i%123 == 0 {
			tr.sane()
		}
	}
	tr.sane()
	sortItems(keys)
	var n int
	tr.Scan(func(item kind) bool {
		n++
		return false
	})
	if n != 1 {
		t.Fatalf("expected 1, got %d", n)
	}

	n = 0
	tr.Scan(func(item kind) bool {
		if !eq(item, keys[n]) {
			t.Fatalf("expected %v, got %v", keys[n], item)
		}
		n++
		return true
	})
	if n != len(keys) {
		t.Fatalf("expected %d, got %d", len(keys), n)
	}
	if tr.Len() != len(keys) {
		t.Fatalf("expected %d, got %d", tr.Len(), len(keys))
	}

	for i := 0; i < tr.Len(); i++ {
		if v, ok := tr.GetAt(i); !ok || !eq(v, keys[i]) {
			t.Fatalf("expected %v, got %v", keys[i], v)
		}
	}

	n = 0
	tr.Reverse(func(item kind) bool {
		n++
		return false
	})
	if n != 1 {
		t.Fatalf("expected 1, got %d", n)
	}
	n = 0
	tr.Reverse(func(item kind) bool {
		if !eq(item, keys[len(keys)-n-1]) {
			t.Fatalf("expected %v, got %v", keys[len(keys)-n-1], item)
		}
		n++
		return true
	})
	if n != len(keys) {
		t.Fatalf("expected %d, got %d", len(keys), n)
	}
	if tr.Len() != len(keys) {
		t.Fatalf("expected %d, got %d", tr.Len(), len(keys))
	}

	tr.sane()

	n = 0
	for i := 0; i < 1000; i++ {
		n := 0
		tr.Scan(func(item kind) bool {
			if n == i {
				return false
			}
			n++
			return true
		})
		if n != i {
			t.Fatalf("expected %d, got %d", i, n)
		}
	}

	n = 0
	for i := 0; i < 1000; i++ {
		n = 0
		tr.Reverse(func(item kind) bool {
			if n == i {
				return false
			}
			n++
			return true
		})
		if n != i {
			t.Fatalf("expected %d, got %d", i, n)
		}
	}

	sortItems(keys)
	for i := 0; i < len(keys); i++ {
		var res kind
		var j int
		tr.Ascend(keys[i], func(item kind) bool {
			if j == 0 {
				res = item
			}
			j++
			return j == i%500
		})
		if !eq(res, keys[i]) {
			t.Fatal("not equal")
		}
	}
	for i := len(keys) - 1; i >= 0; i-- {
		var res kind
		var j int
		tr.Descend(keys[i], func(item kind) bool {
			if j == 0 {
				res = item
			}
			j++
			return j == i%500
		})
		if !eq(res, keys[i]) {
			t.Fatal("not equal")
		}
	}

	if tr.Height() == 0 {
		t.Fatalf("expected non-zero")
	}
	if v, ok := tr.Min(); !ok || !eq(v, keys[0]) {
		t.Fatalf("expected '%v', got '%v'", keys[0], v)
	}
	if v, ok := tr.Max(); !ok || !eq(v, keys[len(keys)-1]) {
		t.Fatalf("expected '%v', got '%v'", keys[len(keys)-1], v)
	}
	if v, ok := tr.PopMin(); !ok || !eq(v, keys[0]) {
		t.Fatalf("expected '%v', got '%v'", keys[0], v)
	}
	tr.sane()
	if v, ok := tr.PopMax(); !ok || !eq(v, keys[len(keys)-1]) {
		t.Fatalf("expected '%v', got '%v'", keys[len(keys)-1], v)
	}
	tr.sane()
	tr.Set(keys[0])
	tr.Set(keys[len(keys)-1])
	shuffleItems(keys)
	var hint bPathHint
	for i := 0; i < len(keys); i++ {
		if v, ok := tr.Get(keys[i]); !ok || !eq(v, keys[i]) {
			t.Fatalf("expected '%v', got '%v'", keys[i], v)
		}
		if v, ok := tr.GetHint(keys[i], &hint); !ok || !eq(v, keys[i]) {
			t.Fatalf("expected '%v', got '%v'", keys[i], v)
		}
	}
	sortItems(keys)
	for i := 0; i < len(keys); i++ {
		if v, ok := tr.PopMin(); !ok || !eq(v, keys[i]) {
			t.Fatalf("expected '%v', got '%v'", keys[i], v)
		}
	}
	for i := 0; i < len(keys); i++ {
		if v, ok := tr.Set(keys[i]); ok || !eq(v, tr.empty) {
			t.Fatalf("expected nil")
		}
	}
	for i := len(keys) - 1; i >= 0; i-- {
		if v, ok := tr.PopMax(); !ok || !eq(v, keys[i]) {
			t.Fatalf("expected '%v', got '%v'", keys[i], v)
		}
	}
	for i := 0; i < len(keys); i++ {
		if v, ok := tr.Set(keys[i]); ok || !eq(v, tr.empty) {
			t.Fatalf("expected nil")
		}
	}
	if v, ok := tr.Delete(testMakeItem(-1)); ok || !eq(v, tr.empty) {
		t.Fatal("expected nil")
	}
	tr.sane()
	shuffleItems(keys)
	if v, ok := tr.Delete(keys[len(keys)/2]); !ok || !eq(v, keys[len(keys)/2]) {
		t.Fatalf("expected '%v', got '%v'", keys[len(keys)/2], v)
	}
	tr.sane()
	if v, ok := tr.Delete(keys[len(keys)/2]); ok || !eq(v, tr.empty) {
		t.Fatalf("expected '%v', got '%v'", tr.empty, v)
	}
	tr.sane()
	tr.Set(keys[len(keys)/2])
	tr.sane()
	for i := 0; i < len(keys); i++ {
		if v, ok := tr.Delete(keys[i]); !ok || !eq(v, keys[i]) {
			t.Fatalf("expected '%v', got '%v'", keys[i], v)
		}
		if v, ok := tr.Get(keys[i]); ok || !eq(v, tr.empty) {
			t.Fatalf("expected nil")
		}
		if v, ok := tr.GetHint(keys[i], &hint); ok || !eq(v, tr.empty) {
			t.Fatalf("expected nil")
		}
		if i%97 == 0 {
			tr.sane()
		}
	}
	if tr.Height() != 0 {
		t.Fatalf("expected 0, got %d", tr.Height())
	}
	shuffleItems(keys)
	for i := 0; i < len(keys); i++ {
		if v, ok := tr.Load(keys[i]); ok || !eq(v, tr.empty) {
			t.Fatalf("expected nil")
		}
		if i%97 == 0 {
			tr.sane()
		}
	}
	for i := 0; i < len(keys); i++ {
		if v, ok := tr.Get(keys[i]); !ok || !eq(v, keys[i]) {
			t.Fatalf("expected '%v', got '%v'", keys[i], v)
		}
	}
	shuffleItems(keys)
	for i := 0; i < len(keys); i++ {
		if v, ok := tr.Delete(keys[i]); !ok || !eq(v, keys[i]) {
			t.Fatalf("expected '%v', got '%v'", keys[i], v)
		}
		if v, ok := tr.Get(keys[i]); ok || !eq(v, tr.empty) {
			t.Fatalf("expected nil")
		}
	}
	sortItems(keys)
	for i := 0; i < len(keys); i++ {
		if v, ok := tr.Load(keys[i]); ok || !eq(v, tr.empty) {
			t.Fatalf("expected nil")
		}
		if i%97 == 0 {
			tr.sane()
		}
	}
	shuffleItems(keys)
	if v, ok := tr.Load(keys[0]); !ok || !eq(v, keys[0]) {
		t.Fatalf("expected '%v', got '%v'", keys[0], v)
	}
	tr.sane()
}

func TestLess(t *testing.T) {
	tr := testNewBTree()
	if !tr.Less(testMakeItem(1), testMakeItem(2)) {
		panic("invalid")
	}
	if tr.Less(testMakeItem(2), testMakeItem(1)) {
		panic("invalid")
	}
	if tr.Less(testMakeItem(1), testMakeItem(1)) {
		panic("invalid")
	}
}

func TestDeleteRandom(t *testing.T) {
	N := 2_000_000
	tr := testNewBTree()
	for i := 0; i < N; i++ {
		tr.Load(testMakeItem(i))
	}
	tr.sane()
	for tr.Len() > 0 {
		var item kind
		var ok bool
		switch rand.Int() % 3 {
		case 0:
			item, ok = tr.GetAt(tr.Len() / 2)
		case 1:
			item, ok = tr.Min()
		case 2:
			item, ok = tr.Max()
		}
		if !ok {
			panic("!")
		}
		v, ok := tr.Delete(item)
		if !ok || !eq(v, item) {
			panic("!")
		}
	}
}

func TestDeleteAt(t *testing.T) {
	N := 10_000
	tr := testNewBTree()
	keys := randKeys(N)
	for _, key := range keys {
		tr.Set(key)
	}
	tr.sane()
	for tr.Len() > 0 {
		index := rand.Intn(tr.Len())
		item1, ok1 := tr.GetAt(index)
		item2, ok2 := tr.DeleteAt(index)
		if !ok1 || !ok2 || !eq(item1, item2) {
			panic("mismatch")
		}
		tr.sane()
	}
}

func TestCopy(t *testing.T) {
	items := randKeys(100000)
	itemsM := testNewBTree()
	for i := 0; i < len(items); i++ {
		itemsM.Set(items[i])
	}
	tr := testNewBTree()
	for i := 0; i < len(items); i++ {
		tr.Set(items[i])
	}
	var wait int32
	var testCopyDeep func(tr *bTree, parent bool)

	testCopyDeep = func(tr *bTree, parent bool) {
		defer func() { atomic.AddInt32(&wait, -1) }()
		if parent {
			// 2 grandchildren
			for i := 0; i < 2; i++ {
				atomic.AddInt32(&wait, 1)
				go testCopyDeep(tr.Copy(), false)
			}
		}

		items2 := make([]kind, 10000)
		for i := 0; i < len(items2); i++ {
			x := testMakeItem(rand.Int())
			_, ok := itemsM.Get(x)
			for ok {
				x = testMakeItem(rand.Int())
				_, ok = itemsM.Get(x)
			}
			items2[i] = x
		}
		for i := 0; i < len(items2); i++ {
			if v, ok := tr.Set(items2[i]); ok || !eq(v, tr.empty) {
				panic("!")
			}
		}
		tr.sane()
		if tr.Len() != len(items)+len(items2) {
			panic("!")
		}
		for i := 0; i < len(items); i++ {
			if v, ok := tr.Get(items[i]); !ok || !eq(v, items[i]) {
				panic("!")
			}
		}
		for i := 0; i < len(items2); i++ {
			if v, ok := tr.Get(items2[i]); !ok || !eq(v, items2[i]) {
				panic("!")
			}
		}

		for i := 0; i < len(items); i++ {
			if v, ok := tr.Delete(items[i]); !ok || !eq(v, items[i]) {
				panic("!")
			}
		}
		tr.sane()
		if tr.Len() != len(items2) {
			panic("!")
		}
		for i := 0; i < len(items2); i++ {
			if v, ok := tr.Get(items2[i]); !ok || !eq(v, items2[i]) {
				panic("!")
			}
		}
		sortItems(items2)
		var i int
		for len(items2) > 0 {
			if i%2 == 0 {
				if v, ok := tr.PopMin(); !ok || !eq(v, items2[0]) {
					panic("!")
				}
				items2 = items2[1:]
			} else {
				if v, ok := tr.PopMax(); !ok || !eq(v, items2[len(items2)-1]) {
					panic("!")
				}
				items2 = items2[:len(items2)-1]
			}
			if i%123 == 0 {
				tr.sane()
				if tr.Len() != len(items2) {
					panic("!")
				}
				for i := 0; i < len(items2); i++ {
					if v, ok := tr.Get(items2[i]); !ok || !eq(v, items2[i]) {
						panic("!")
					}
				}
			}
			i++
		}
		tr.sane()
		if tr.Len() != len(items2) {
			panic("!")
		}
	}

	// 10 children
	for i := 0; i < 10; i++ {
		atomic.AddInt32(&wait, 1)
		go testCopyDeep(tr.Copy(), true)
	}

	for atomic.LoadInt32(&wait) > 0 {
		tr.sane()
		if tr.Len() != len(items) {
			panic("!")
		}
		for i := 0; i < len(items); i++ {
			if v, ok := tr.Get(items[i]); !ok || !eq(v, items[i]) {
				panic("!")
			}
		}
		runtime.Gosched()
	}
}

func TestVarious(t *testing.T) {
	N := 1_000_000
	tr := testNewBTree()
	var hint bPathHint
	for _, i := range randKeys(N) {
		if v, ok := tr.SetHint(i, &hint); ok || !eq(v, tr.empty) {
			panic("!")
		}
	}
	for _, i := range randKeys(N) {
		if v, ok := tr.GetHint(i, &hint); !ok || !eq(v, i) {
			panic("!")
		}
	}
	for _, i := range randKeys(N) {
		if v, ok := tr.DeleteHint(i, &hint); !ok || !eq(v, i) {
			panic("!")
		}
	}
	if v, ok := tr.DeleteAt(0); ok || !eq(v, tr.empty) {
		panic("!")
	}
	if v, ok := tr.GetAt(0); ok || !eq(v, tr.empty) {
		panic("!")
	}
	for i := 0; i < N; i++ {
		item := testMakeItem(i)
		if v, ok := tr.SetHint(item, &hint); ok || !eq(v, tr.empty) {
			panic("!")
		}
		item = testMakeItem(i)
		if v, ok := tr.SetHint(item, &hint); !ok || !eq(v, item) {
			panic("!")
		}
		item = testMakeItem(i)
		if v, ok := tr.SetHint(item, &hint); !ok || !eq(v, item) {
			panic("!")
		}
	}
	for i := 0; i < N; i++ {
		item := testMakeItem(i)
		if v, ok := tr.GetHint(item, &hint); !ok || !eq(v, item) {
			panic("!")
		}
	}
	for i := 0; i < 100; i++ {
		var count int
		tr.Walk(func(_ []kind) bool {
			if count == i {
				return false
			}
			count++
			return true
		})
	}

	for i := 0; i < N; i++ {
		item := testMakeItem(i)
		if v, ok := tr.DeleteHint(item, &hint); !ok || !eq(v, item) {
			panic("!")
		}
	}
}

func (tr *bTree) sane() {
	if err := tr.Sane(); err != nil {
		panic(err)
	}
}

type saneError string

func (err saneError) Error() string {
	return string(err)
}

// btree_sane returns true if the entire btree and every node are valid.
// - height of all leaves are the equal to the btree height.
// - deep count matches the btree count.
// - all nodes have the correct number of items and counts.
// - all items are in order.
func (tr *bTree) Sane() error {
	if tr == nil {
		return nil
	}
	if !tr.saneheight() {
		return saneError("!sane-height")
	}
	if tr.Len() != tr.count || tr.deepcount() != tr.count {
		return saneError("!sane-count")
	}
	if !tr.saneprops() {
		return saneError("!sane-props")
	}
	if !tr.saneorder() {
		return saneError("!sane-order")
	}
	if !tr.sanenils() {
		return saneError("!sane-nils")
	}
	return nil
}

// btree_saneheight returns true if the height of all leaves match the height
// of the btree.
func (tr *bTree) saneheight() bool {
	height := tr.Height()
	if tr.root != nil {
		if height == 0 {
			return false
		}
		return tr.root.saneheight(1, height)
	}
	return height == 0
}

func (n *node) saneheight(height, maxheight int) bool {
	if n.leaf() {
		if height != maxheight {
			return false
		}
	} else {
		i := 0
		for ; i < len(n.items); i++ {
			if !(*n.children)[i].saneheight(height+1, maxheight) {
				return false
			}
		}
		if !(*n.children)[i].saneheight(height+1, maxheight) {
			return false
		}
	}
	return true
}

// btree_deepcount returns the number of items in the btree.
func (tr *bTree) deepcount() int {
	if tr.root != nil {
		return tr.root.deepcount()
	}
	return 0
}

func (n *node) deepcount() int {
	count := len(n.items)
	if !n.leaf() {
		for i := 0; i <= len(n.items); i++ {
			count += (*n.children)[i].deepcount()
		}
	}
	if n.count != count {
		return -1
	}
	return count
}

func (tr *bTree) nodesaneprops(n *node, height int) bool {
	if height == 1 {
		if len(n.items) < 1 || len(n.items) > maxItems {
			println(len(n.items) < 1)
			return false
		}
	} else {
		if len(n.items) < minItems || len(n.items) > maxItems {
			println(2)
			return false
		}
	}
	if !n.leaf() {
		if len(*n.children) != len(n.items)+1 {
			println(3)
			return false
		}
		for i := 0; i < len(n.items); i++ {
			if !tr.nodesaneprops((*n.children)[i], height+1) {
				println(4)
				return false
			}
		}
		if !tr.nodesaneprops((*n.children)[len(n.items)], height+1) {
			println(5)
			return false
		}
	}
	return true
}

func (tr *bTree) saneprops() bool {
	if tr.root != nil {
		return tr.nodesaneprops(tr.root, 1)
	}
	return true
}

func (tr *bTree) sanenilsnode(n *node) bool {
	items := n.items[:cap(n.items):cap(n.items)]
	for i := len(n.items); i < len(items); i++ {
		if items[i] != tr.empty {
			return false
		}
	}
	if !n.leaf() {
		for i := 0; i < len(*n.children); i++ {
			if (*n.children)[i] == nil {
				return false
			}
		}
		children := (*n.children)[:cap(*n.children):cap(*n.children)]
		for i := len(*n.children); i < len(children); i++ {
			if children[i] != nil {
				return false
			}
		}
		for i := 0; i < len(*n.children); i++ {
			if !tr.sanenilsnode((*n.children)[i]) {
				return false
			}
		}
	}
	return true
}

// sanenils checks that all the slots in the item slice that are not used,
//   n.items[len(n.items):cap(n.items):cap(n.items)]
// are equal to the empty value of the kind.
func (tr *bTree) sanenils() bool {
	if tr.root != nil {
		return tr.sanenilsnode(tr.root)
	}
	return true
}

func (tr *bTree) saneorder() bool {
	var last kind
	var count int
	var bad bool
	tr.Walk(func(items []kind) bool {
		for _, item := range items {
			if count > 0 {
				if !less(last, item, tr.ctx) {
					bad = true
					return false
				}
			}
			last = item
			count++
		}
		return true
	})
	return !bad && count == tr.count
}

func TestIter(t *testing.T) {
	N := 100_000
	tr := testNewBTree()
	var all []kind
	for i := 0; i < N; i++ {
		tr.Load(testMakeItem(i))
		all = append(all, testMakeItem(i))
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
		if eq(iter.Item(), testMakeItem(N/2)) {
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
