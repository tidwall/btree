### This is an experimental branch that enables generics. Requires Go 1.18 and above.

# btree

[![GoDoc](https://godoc.org/github.com/tidwall/btree?status.svg)](https://godoc.org/github.com/tidwall/btree)

An [efficient](#performance) [B-tree](https://en.wikipedia.org/wiki/B-tree) implementation in Go.

## Features

- Support for Generics (Go 1.18).
- `Copy()` method with copy-on-write support.
- Fast bulk loading for pre-ordered data using the `Load()` method.
- All operations are thread-safe.
- [Path hinting](PATH_HINT.md) optimization for operations with nearby keys.

## Installing

To start using btree, install Go and run `go get`:

```sh
$ go get -u github.com/tidwall/btree@generics
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
func byKeys(a, b Item) bool {
	return a.Key < b.Key
}

// byVals is a comparison function that compares item values and returns true
// when a is less than b.
func byVals(a, b Item) bool {
	if a.Val < b.Val {
		return true
	}
	if a.Val > b.Val {
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
	keys := btree.New[Item](byKeys)
	vals := btree.New[Item](byVals)

	// Create some items.
	users := []Item{
		Item{Key: "user:1", Val: "Jane"},
		Item{Key: "user:2", Val: "Andy"},
		Item{Key: "user:3", Val: "Steve"},
		Item{Key: "user:4", Val: "Andrea"},
		Item{Key: "user:5", Val: "Janet"},
		Item{Key: "user:6", Val: "Andy"},
	}

	// Insert each user into both trees
	for _, user := range users {
		keys.Set(user)
		vals.Set(user)
	}

	// Iterate over each user in the key tree
	keys.Scan(func(item Item) bool {
		fmt.Printf("%s %s\n", item.Key, item.Val)
		return true
	})

	fmt.Printf("\n")
	// Iterate over each user in the val tree
	vals.Scan(func(item Item) bool {
		fmt.Printf("%s %s\n", item.Key, item.Val)
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

- `google`: The [google/btree](https://github.com/google/btree) package (without generics)
- `tidwall`: The [tidwall/btree](https://github.com/tidwall/btree) package (without generics)
- `tidwall(G)`: The [tidwall/btree](https://github.com/tidwall/btree) package (with generics)
- `go-arr`: A simple Go array

The following benchmarks were run on my 2019 Macbook Pro (2.4 GHz 8-Core Intel Core i9) 
using Go Development version 1.18-8ff254e3 (gotip).
The items are simple 8-byte ints. 

```
** sequential set **
google:     set-seq        1,000,000 ops in 157ms, 6,352,361/sec, 157 ns/op, 39.0 MB, 40 bytes/op
tidwall:    set-seq        1,000,000 ops in 144ms, 6,934,619/sec, 144 ns/op, 23.5 MB, 24 bytes/op
tidwall(G): set-seq        1,000,000 ops in 82ms, 12,205,826/sec, 81 ns/op, 8.2 MB, 8 bytes/op
tidwall:    set-seq-hint   1,000,000 ops in 75ms, 13,256,139/sec, 75 ns/op, 23.5 MB, 24 bytes/op
tidwall(G): set-seq-hint   1,000,000 ops in 47ms, 21,073,524/sec, 47 ns/op, 8.2 MB, 8 bytes/op
tidwall:    load-seq       1,000,000 ops in 47ms, 21,438,912/sec, 46 ns/op, 23.5 MB, 24 bytes/op
tidwall(G): load-seq       1,000,000 ops in 23ms, 43,405,780/sec, 23 ns/op, 8.2 MB, 8 bytes/op
go-arr:     append         1,000,000 ops in 25ms, 40,684,212/sec, 24 ns/op

** random set **
google:     set-rand       1,000,000 ops in 593ms, 1,685,165/sec, 593 ns/op, 29.7 MB, 31 bytes/op
tidwall:    set-rand       1,000,000 ops in 571ms, 1,751,559/sec, 570 ns/op, 29.6 MB, 31 bytes/op
tidwall(G): set-rand       1,000,000 ops in 232ms, 4,307,484/sec, 232 ns/op, 11.2 MB, 11 bytes/op
tidwall:    set-rand-hint  1,000,000 ops in 632ms, 1,582,840/sec, 631 ns/op, 29.6 MB, 31 bytes/op
tidwall(G): set-rand-hint  1,000,000 ops in 267ms, 3,752,093/sec, 266 ns/op, 11.2 MB, 11 bytes/op
tidwall:    set-again      1,000,000 ops in 682ms, 1,466,345/sec, 681 ns/op
tidwall(G): set-again      1,000,000 ops in 244ms, 4,092,881/sec, 244 ns/op
tidwall(:   set-after-copy 1,000,000 ops in 679ms, 1,472,038/sec, 679 ns/op
tidwall(G): set-after-copy 1,000,000 ops in 242ms, 4,139,555/sec, 241 ns/op
tidwall:    load-rand      1,000,000 ops in 591ms, 1,691,903/sec, 591 ns/op, 29.6 MB, 31 bytes/op
tidwall(G): load-rand      1,000,000 ops in 257ms, 3,893,858/sec, 256 ns/op, 11.2 MB, 11 bytes/op

** sequential get **
google:     get-seq        1,000,000 ops in 173ms, 5,772,752/sec, 173 ns/op
tidwall:    get-seq        1,000,000 ops in 164ms, 6,099,748/sec, 163 ns/op
tidwall(G): get-seq        1,000,000 ops in 82ms, 12,212,668/sec, 81 ns/op
tidwall:    get-seq-hint   1,000,000 ops in 84ms, 11,959,334/sec, 83 ns/op
tidwall(G): get-seq-hint   1,000,000 ops in 36ms, 27,856,690/sec, 35 ns/op

** random get **
google:     get-rand       1,000,000 ops in 708ms, 1,412,173/sec, 708 ns/op
tidwall:    get-rand       1,000,000 ops in 694ms, 1,441,070/sec, 693 ns/op
tidwall(G): get-rand       1,000,000 ops in 246ms, 4,064,635/sec, 246 ns/op
tidwall:    get-rand-hint  1,000,000 ops in 775ms, 1,290,195/sec, 775 ns/op
tidwall(G): get-rand-hint  1,000,000 ops in 280ms, 3,575,213/sec, 279 ns/op
```

*You can find the benchmark utility at [tidwall/btree-benchmark](https://github.com/tidwall/btree-benchmark/tree/generics)*

## Contact

Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

Source code is available under the MIT [License](/LICENSE).
