# btree

[![GoDoc](https://godoc.org/github.com/tidwall/btree?status.svg)](https://godoc.org/github.com/tidwall/btree)

An [efficient](#performance) [B-tree](https://en.wikipedia.org/wiki/B-tree) implementation in Go. 

## Features

- `Copy()` method with copy-on-write support.
- Fast bulk loading for pre-ordered data using the `Load()` method.
- All operations are thread-safe.
- [Path hinting](PATH_HINT.md) optimization for operations with nearby keys.

## Installing

To start using btree, install Go and run `go get`:

```sh
$ go get -u github.com/tidwall/btree
```

## Usage

```go
package main

import (
	"fmt"

	"github.com/tidwall/btree"
)

type Item struct {
	Key, Val string
}

// byKeys is a comparison function that compares item keys and returns true
// when a is less than b.
func byKeys(a, b interface{}) bool {
	i1, i2 := a.(*Item), b.(*Item)
	return i1.Key < i2.Key
}

// byVals is a comparison function that compares item values and returns true
// when a is less than b.
func byVals(a, b interface{}) bool {
	i1, i2 := a.(*Item), b.(*Item)
	if i1.Val < i2.Val {
		return true
	}
	if i1.Val > i2.Val {
		return false
	}
	// Both vals are equal so we should fall though
	// and let the key comparison take over.
	return byKeys(a, b)
}

func main() {
	// Create a tree for keys and a tree for values.
	// The "keys" tree will be sorted on the Keys field.
	// The "values" tree will be sorted on the Values field.
	keys := btree.New(byKeys)
	vals := btree.New(byVals)

	// Create some items.
	users := []*Item{
		&Item{Key: "user:1", Val: "Jane"},
		&Item{Key: "user:2", Val: "Andy"},
		&Item{Key: "user:3", Val: "Steve"},
		&Item{Key: "user:4", Val: "Andrea"},
		&Item{Key: "user:5", Val: "Janet"},
		&Item{Key: "user:6", Val: "Andy"},
	}

	// Insert each user into both trees
	for _, user := range users {
		keys.Set(user)
		vals.Set(user)
	}

	// Iterate over each user in the key tree
	keys.Ascend(nil, func(item interface{}) bool {
		kvi := item.(*Item)
		fmt.Printf("%s %s\n", kvi.Key, kvi.Val)
		return true
	})

	fmt.Printf("\n")
	// Iterate over each user in the val tree
	vals.Ascend(nil, func(item interface{}) bool {
		kvi := item.(*Item)
		fmt.Printf("%s %s\n", kvi.Key, kvi.Val)
		return true
	})

	// Output:
	// user:1 Jane
	// user:2 Andy
	// user:3 Steve
	// user:4 Andrea
	// user:5 Janet
	// user:6 Andy
	//
	// user:4 Andrea
	// user:2 Andy
	// user:6 Andy
	// user:1 Jane
	// user:5 Janet
	// user:3 Steve
}
```

## Operations

### Basic

```
Len()                   # return the number of items in the btree
Set(item)               # insert or replace an existing item
Get(item)               # get an existing item
Delete(item)            # delete an item
```

### Iteration

```
Ascend(pivot, iter)     # scan items in ascending order starting at pivot.
Descend(pivot, iter)    # scan items in descending order starting at pivot.
```

### Queues

```
Min()                   # return the first item in the btree
Max()                   # return the last item in the btree
PopMin()                # remove and return the first item in the btree
PopMax()                # remove and return the last item in the btree
```
### Bulk loading

```
Load(item)              # load presorted items into tree
```

### Path hints

```
SetHint(item, *hint)    # insert or replace an existing item
GetHint(item, *hint)    # get an existing item
DeleteHint(item, *hint) # delete an item
```

### Array-like operations

```
GetAt(index)     # returns the value at index
DeleteAt(index)  # deletes the item at index
```

## Performance

This implementation was designed with performance in mind. 

The following benchmarks were run on my 2019 Macbook Pro (2.4 GHz 8-Core Intel Core i9) using Go 1.17. The items are simple 8-byte ints. 

- `google`: The [google/btree](https://github.com/google/btree) package
- `tidwall`: The [tidwall/btree](https://github.com/tidwall/btree) package
- `go-arr`: Just a simple Go array

```
** sequential set **
google:  set-seq        1,000,000 ops in 162ms, 6,183,760/sec, 161 ns/op, 38.6 MB, 40 bytes/op
tidwall: set-seq        1,000,000 ops in 148ms, 6,755,156/sec, 148 ns/op, 44.3 MB, 46 bytes/op
tidwall: set-seq-hint   1,000,000 ops in 66ms, 15,100,485/sec, 66 ns/op, 44.3 MB, 46 bytes/op
tidwall: load-seq       1,000,000 ops in 39ms, 25,510,441/sec, 39 ns/op, 44.3 MB, 46 bytes/op
go-arr:  append         1,000,000 ops in 69ms, 14,454,379/sec, 69 ns/op, 424 bytes, 0 bytes/op

** random set **
google:  set-rand       1,000,000 ops in 653ms, 1,530,252/sec, 653 ns/op, 29.1 MB, 30 bytes/op
tidwall: set-rand       1,000,000 ops in 610ms, 1,638,596/sec, 610 ns/op, 34.3 MB, 35 bytes/op
tidwall: set-rand-hint  1,000,000 ops in 615ms, 1,624,795/sec, 615 ns/op, 34.0 MB, 35 bytes/op
tidwall: set-again      1,000,000 ops in 786ms, 1,272,125/sec, 786 ns/op
tidwall: set-after-copy 1,000,000 ops in 791ms, 1,263,949/sec, 791 ns/op
tidwall: load-rand      1,000,000 ops in 542ms, 1,844,838/sec, 542 ns/op, 33.7 MB, 35 bytes/op

** sequential get **
google:  get-seq        1,000,000 ops in 161ms, 6,216,649/sec, 160 ns/op
tidwall: get-seq        1,000,000 ops in 132ms, 7,548,647/sec, 132 ns/op
tidwall: get-seq-hint   1,000,000 ops in 64ms, 15,716,405/sec, 63 ns/op

** random get **
google:  get-rand       1,000,000 ops in 662ms, 1,511,374/sec, 661 ns/op
tidwall: get-rand       1,000,000 ops in 702ms, 1,424,161/sec, 702 ns/op
tidwall: get-rand-hint  1,000,000 ops in 792ms, 1,263,192/sec, 791 ns/op
```

*You can find the benchmark utility at [tidwall/btree-benchmark](https://github.com/tidwall/btree-benchmark)*

## Contact

Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

Source code is available under the MIT [License](/LICENSE).
