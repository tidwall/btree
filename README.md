# btree

[![GoDoc](https://godoc.org/github.com/tidwall/btree?status.svg)](https://godoc.org/github.com/tidwall/btree)

An efficient [B-tree](https://en.wikipedia.org/wiki/B-tree) for Go.

## Features

- Includes a variety of common [collections types](#types). All backed by a btree structure
- Modern Go ergonomics with a friendly API
- Adopts Go [generic collection types](https://github.com/golang/go/issues/80590) -- `_Abstract(Collection/Map/Set)`
- All data operations are [O(log n)](https://en.wikipedia.org/wiki/Time_complexity#Logarithmic_time) complexity
- Instant copy-on-write (shadow clones), providing O(1) snapshots
- Uses [btree counting](https://www.chiark.greenend.org.uk/~sgtatham/algorithms/cbtree.html) for O(log n) random access
- Exhaustively tested code with 100% coverage
- Optimized for [high performance](#performance) and low memory

## Types

- [`Map`](#map): Key value pairs. Sorting ordered by key
- [`Set`](#set): Like Map, but only for storing keys. No values
- [`Array`](#array): Dynamic array of unsorted data
- [`Table`](#table): Data sorted by key fields or a custom compare function
- [`Stack`](#stack): LIFO (last-in, first-out) data structure
- [`Queue`](#queue): FIFO (first-in, first-out) data structure
- [`Deque`](#deque): Double-ended queue
- [`Prique`](#prique): Priority queue

## Using 

```
go get github.com/tidwall/btree/v2
```

<!---------------------------------------------------------------------------
-- Map
----------------------------------------------------------------------------->

## Map

btree.Map is a sorted associative collection of key-value pairs with unique keys. 
The keys adhere to the parameter type [`cmp.Ordered`](https://pkg.go.dev/cmp#Ordered) and are naturally sorted using [`cmp.Compare`](https://pkg.go.dev/cmp#Compare).

### Operations

```py
Insert(key, val)        # Insert an item. (does not replace if already exists)
Replace(key, val)       # Replace an existing item. (does not insert if not exists)
Set(key, val)           # Insert or replace an item.
Lookup(key)             # Get an existing item, returning false if not exists
Get(key)                # Get an existing item.
Contains(key)           # Test if an item exists.
Delete(key)             # Remove an item.

Seek(key)               # Searches for the first item that is >= to key.
SeekNext(key)           # Searches for the first item that is > key.
SeekPrev(key)           # Searches for the first item that is < key.

All()                   # Iterate items in ascending order.            (iter.Seq2[K,V])
Backward()              # Iterate items in descending order.           (iter.Seq2[K,V])
Ascend(key)             # Iterate items in ascending order >= to key.  (iter.Seq2[K,V])
Descend(key)            # Iterate items in descending order <= to key. (iter.Seq2[K,V])
Keys()                  # Iterate key only in ascending order.         (iter.Seq[K])
Values()                # Iterate values only in ascending order.      (iter.Seq[K])
Drain()                 # Iterate and remove in ascending order.       (iter.Seq2[K,V])
DrainBackward()         # Iterate and remove in descending order.      (iter.Seq2[K,V])

PushFront(key, val)     # Insert item to front of map.
PushBack(key, val)      # Insert item to back of map.
PopFront()              # Remove the first item.
PopBack()               # Remove the last item.
Front()                 # Get the first item.
Back()                  # Get the last item.
PopFrontFunc(cond)      # Remove first item if cond returns true.
PopBackFunc(cond)       # Remove last item if cond returns true.

InsertAt(i, key, val)   # Inserts item at index. (collection size grows by one)
ReplaceAt(i, key, val)  # Replace an item at index.
GetAt(i)                # Get an item at index.
IndexOf(key)            # Get the index of an item.
DeleteAt(i)             # Remove an item at index.
AscendAt(i)             # Iterate items in ascending order >= to index.  (iter.Seq2[K,V])
DescendAt(i)            # Iterate items in descending order <= to index. (iter.Seq2[K,V])

DeleteRange(min, max)   # Remove items within the provided sub-range. [min,max)
DeleteRangeAt(i, count) # Remove items starting at index.

Action(key, actfunc)    # Perform action (replace/delete) using custom function
DeleteFunc(delfunc)     # Delete item using custom function

Len()                   # Get the number of items in map.
Clone()                 # Clone map, fast O(1), uses copy-on-write shadow cloning.
Clear()                 # Remove all items from map
Release()               # Same as Clear() but optimized for cloned collections.

String()                # String representation
```

### Example

```go
package main

import (
	"fmt"

	"github.com/tidwall/btree/v2"
)

func main() {
	// Create a map
	var users btree.Map[string, string]

	// Add some users
	users.Insert("user:4", "Andrea")
	users.Insert("user:6", "Andy")
	users.Insert("user:2", "Andy")
	users.Insert("user:1", "Jane")
	users.Insert("user:5", "Janet")
	users.Insert("user:3", "Steve")

	// Iterate over the map and print each user
	for key, value := range users.All() {
		fmt.Printf("%s %s\n", key, value)
	}
	fmt.Printf("\n")

	// Delete a couple users
	users.Delete("user:5")
	users.Delete("user:1")

	// Print the map again
	for key, value := range users.All() {
		fmt.Printf("%s %s\n", key, value)
	}
	fmt.Printf("\n")
}

// Output:
// user:1 Jane
// user:2 Andy
// user:3 Steve
// user:4 Andrea
// user:5 Janet
// user:6 Andy
//
// user:2 Andy
// user:3 Steve
// user:4 Andrea
// user:6 Andy
```

<!---------------------------------------------------------------------------
-- Set
----------------------------------------------------------------------------->

## Set

btree.Set is an associative collection of unique sorted keys. 
The keys adhere to the parameter type [`cmp.Ordered`](https://pkg.go.dev/cmp#Ordered) and are naturally sorted using [`cmp.Compare`](https://pkg.go.dev/cmp#Compare).

### Operations

```py
Insert(key)             # Insert key
Contains(key)           # Check if key exists
Delete(key)             # Remove key
Len()                   # Get the number of keys in the collection

All()                   # Iterate keys in ascending order.            (iter.Seq[K])
Backward()              # Iterate keys in descending order.           (iter.Seq[K])
Ascend(key)             # Iterate keys in ascending order >= to key.  (iter.Seq[K])
Descend(key)            # Iterate keys in descending order <= to key. (iter.Seq[K])
Drain()                 # Iterate and remove in ascending order.      (iter.Seq[K])
DrainBackward()         # Iterate and remove in descending order.     (iter.Seq[K])

Seek(key)               # Searches for the first key that is >= to key
SeekNext(key)           # Searches for the first key that is > key
SeekPrev(key)           # Searches for the first key that is < key

PushFront(key)          # Insert key to front of collection
PushBack(key)           # Insert key to back of collection
PopFront()              # Remove the first key
PopBack()               # Remove the last key
Front()                 # Get the first key
Back()                  # Get the last key

InsertAt(i, key)        # Insert key at index. (collection size grows by one)
ReplaceAt(i, key)       # Replace key at index
GetAt(i)                # Gets key at index
IndexOf(key)            # Get the index of key
DeleteAt(i)             # Remove key at index
AscendAt(i)             # Iterate keys in ascending order >= to index. (iter.Seq[K])
DescendAt(i)            # Iterate key in descending order <= to index. (iter.Seq[K])

DeleteRange(min, max)   # Remove keys within the provided sub-range. [min,max)
DeleteRangeAt(i, count) # Remove keys starting at index

DeleteFunc(delfunc)     # Delete item using custom function

Len()                   # Get the number of items in collection
Clone()                 # Clone collection, fast O(1), uses Copy-on-write shadow cloning
Clear()                 # Remove all items from collection
Release()               # Same as Clear() but optimized for cloned collections

String()                # String representation

Difference(s2)          # Returns new set of keys that are in set but not in other
DifferenceWith(s2)      # Removes any key from set that are in other
Equal(s2)               # Check if two sets are equal
Intersection(s2)        # Returns new set containing item that are in both sets
IntersectionWith(s2)    # Removes any items from set that are not in other
Intersects(s2)          # Checks if sets share any keys
SymmetricDifference(s2) # Returns new set with items that are in either, but not both sets.
SymmetricDifferenceWith(s2) # Update set to the symmetric difference of both sets
Union(set)              # Returns new set containing all keys of both sets.
UnionWith(set)          # Inserts all keys of other set.

```

```go
package main

import (
	"fmt"

	"github.com/tidwall/btree/v2"
)

func main() {
	// Create a set
	var names btree.Set[string]

	// Add some names
	names.Insert("Jane")
	names.Insert("Andrea")
	names.Insert("Steve")
	names.Insert("Andy")
	names.Insert("Janet")
	names.Insert("Andy")

	// Iterate over the set and print each name
	for key := range names.All() {
		fmt.Printf("%s\n", key)
	}
	fmt.Printf("\n")

	// Delete a couple names
	names.Delete("Steve")
	names.Delete("Andy")

	// Print the names again
	for key := range names.All() {
		fmt.Printf("%s\n", key)
	}
	fmt.Printf("\n")
}

// Output:
// Andrea
// Andy
// Jane
// Janet
// Steve
//
// Andrea
// Jane
// Janet
```

<!---------------------------------------------------------------------------
-- Array
----------------------------------------------------------------------------->

## Array

btree.Array is a dynamic resizable array of unsorted items.
It provides random access with `O(log n)` complexity to all data operations, 
including inserting and deleting items in the middle of the array.

### Operations

```py
Insert(i, item)       # Insert item at index. (collection size grows by one)
Replace(i, item)      # Replace existing item at index.
Lookup(i)             # Gets item at index. Returns false it not exists
Get(i)                # Gets item at index.
Delete(i)             # Remove item at index.
Contains(item)        # Checks if item exists

All()                 # Iterate items in ascending order.              (iter.Seq[T])
Backward()            # Iterate items in descending order.             (iter.Seq[T])
Ascend(i)             # Iterate items in ascending order >= to index.  (iter.Seq[T])
Descend(i)            # Iterate items in descending order <= to index. (iter.Seq[T])
Drain()               # Iterate and remove in ascending order.         (iter.Seq[T])
DrainBackward()       # Iterate and remove in descending order.        (iter.Seq[T])

PushFront(item)       # Insert item to front of collection.
PushBack(item)        # Insert item to back of collection.
PopFront()            # Remove the first item.
PopBack()             # Remove the last item.
Front()               # Get the first item.
Back()                # Get the last item.
PopFrontFunc(cond)    # Remove first item if cond returns true.
PopBackFunc(cond)     # Remove last item if cond returns true.

DeleteRange(i, count) # Remove items starting at index.

Len()                 # Get the number of items in collection.
Clone()               # Clone collection, fast O(1), uses Copy-on-write shadow cloning.
Clear()               # Remove all items from collection
Release()             # Same as Clear() but optimized for cloned collections.

String()              # String representation
```

### Example

```go
package main

import (
	"fmt"

	"github.com/tidwall/btree/v2"
)

func main() {
	// Create an array of names
	var names btree.Array[string]

	// Add some names
	names.Insert(0, "Andrea")
	names.Insert(1, "Tom")
	names.Insert(2, "Andy")
	names.Insert(3, "Jane")
	names.Insert(4, "Janet")
	names.Insert(5, "Steve")

	// Iterate over the array and print each name
	for name := range names.All() {
		fmt.Printf("%s\n", name)
	}
	fmt.Printf("\n")

	// Delete a couple names
	names.Delete(3)
	names.Delete(1)

	// Print the names again
	for name := range names.All() {
		fmt.Printf("%s\n", name)
	}
	fmt.Printf("\n")
}

// Output:
// Andrea
// Tom
// Andy
// Jane
// Janet
// Steve
//
// Andrea
// Andy
// Janet
// Steve
```

<!---------------------------------------------------------------------------
-- Table
----------------------------------------------------------------------------->

## Table

btree.Table is a general purpose btree collection for storing sorted data.

### Features

- Automatic type ordering detection.
- Optional [custom comparator](#custom-comparator)
- [Tagged struct keys](#tagged-struct-keys) (row-like primary and composite keys)

### Automatic type ordering

A btree.Table will automatically detect the ordering of the data type. 

### cmp.Ordered data type

When the data type is `cmp.Ordered` then `cmp.Compare` is used to sort the data.

```go
var names btree.Table[string]

names.Insert("Andrea")
names.Insert("Tom")
names.Insert("Andy")
names.Insert("Jane")
names.Insert("Janet")
names.Insert("Steve")

for name := range names.All() {
	fmt.Printf("%s\n", name)
}

// Output:
// Andrea
// Andy
// Jane
// Janet
// Steve
// Tom
```

### Struct field detection

When the data type is a struct, or pointer to struct, then the data is sorted 
by the first struct field that adheres to `cmp.Ordered`.

```go
type User struct {
	id   int
	name string
}

var users btree.Table[User]
users.Insert(User{4, "Andrea"})
users.Insert(User{6, "Andy"})
users.Insert(User{2, "Andy"})
users.Insert(User{1, "Jane"})
users.Insert(User{5, "Janet"})
users.Insert(User{3, "Steve"})

for user := range users.All() {
	fmt.Printf("%d %s\n", user.id, user.name)
}

// Output:
// 1 Jane
// 2 Andy
// 3 Steve
// 4 Andrea
// 5 Janet
// 6 Andy
```

### Custom comparator

A custom comparator may be used to override automatic detection.

```go
type User struct {
	age  int
	name string
}

// sort by name, then age
users := btree.NewTableOptions(btree.TableOptions[User]{
	Compare: func(a, b User) int {
		c := cmp.Compare(a.name, b.name)
		if c == 0 {
			c = cmp.Compare(a.age, b.age)
		}
		return c
	},
})
users.Insert(User{27, "Andrea"})
users.Insert(User{54, "Andy"})
users.Insert(User{31, "Andy"})
users.Insert(User{43, "Jane"})
users.Insert(User{29, "Janet"})
users.Insert(User{62, "Steve"})

for user := range users.All() {
	fmt.Printf("%d %s\n", user.age, user.name)
}

// Output:
// 27 Andrea
// 31 Andy
// 54 Andy
// 43 Jane
// 29 Janet
// 62 Steve
```

### Tagged struct keys

Structs may include tagged keys, which is a simple and explicit way to
define key order. The syntax resembling a traditional database table.

The tag `btree:"key"` designates that struct field as the key.

```go
// Order by id
type User struct {
	id   int `btree:"key"`
	name string
}
```

A composite key can be made by adding two or more tags.

```go
// Order by last, then first
type User struct {
	last  string `btree:"key"`
	first string `btree:"key"`
}
```

By default, order of the keys are automatic.
But it's possible to define the order manually by adding a `.{index}` to the tag.

```go
// Order by last, then first
type User struct {
	first string `btree:"key.1"`
	last  string `btree:"key.0"`
}
```

Keys may be ordered ascending or descending using `asc` and `desc`, respectively.
Ascending is the default.


```go
// Order by last descending, then first ascending
type User struct {
	last  string `btree:"key,desc"`
	first string `btree:"key,asc"`
}
```

Text collation may be added to string keys.

```go
// Use case-insensitive binary collation.
// Order by last descending, then first ascending.
type User struct {
 	last  string `btree:"key,binary_CI,desc"`
 	first string `btree:"key,binary_CI"`
}
```

Available collations:

- `binary_CS`: Case sensitive binary strings (default)
- `binary_CI`: Case insensitive binary strings
- `utf8_CS`: Case sensitive utf8 unicode
- `utf8_CI`: Case insensitive utf8 unicode

Also `bin_CS`, `CS`, `bin_CI`, and `CI` are available as shorthand for 
`binary_CS` and `binary_CI`.

More collations may be added in the future.

### Operations

Table operations include:

```py
Insert(item)            # Insert an item. (does not replace if already exists)
Replace(item)           # Replace an existing item. (does not insert if not exists)
Set(item)               # Insert or replace an item.
Lookup(key)             # Get an existing item. Return false if not exists
Get(key)                # Get an existing item.
Contains(key)           # Test if an item exists.
Delete(key)             # Remove an item.

Seek(key)               # Searches for the first item that is >= to key.
SeekNext(key)           # Searches for the first item that is > key.
SeekPrev(key)           # Searches for the first item that is < key.

All()                   # Iterate items in ascending order.            (iter.Seq[T])
Backward()              # Iterate items in descending order.           (iter.Seq[T])
Ascend(key)             # Iterate items in ascending order >= to key.  (iter.Seq[T])
Descend(key)            # Iterate items in descending order <= to key. (iter.Seq[T])
Drain()                 # Iterate and remove in ascending order.       (iter.Seq[T])
DrainBackward()         # Iterate and remove in descending order.      (iter.Seq[T])

PushFront(item)         # Insert item to front of table.
PushBack(item)          # Insert item to back of table.
PopFront()              # Remove the first item.
PopBack()               # Remove the last item.
Front()                 # Get the first item.
Back()                  # Get the last item.
PopFrontFunc(cond)        # Remove first item if cond returns true.
PopBackFunc(cond)         # Remove last item if cond returns true.

InsertAt(i, item)       # Inserts item at index. (collection size grows by one)
ReplaceAt(i, item)      # Replace an item at index.
GetAt(i)                # Get an item at index.
IndexOf(key)            # Get the index of an item.
DeleteAt(i)             # Remove an item at index.
AscendAt(i)             # Iterate items in ascending order >= to index.  (iter.Seq[T])
DescendAt(i)            # Iterate items in descending order <= to index. (iter.Seq[T])

DeleteRange(min, max)   # Remove items within the provided sub-range. [min,max)
DeleteRangeAt(i, count) # Remove items starting at index.

Len()                   # Get the number of items in collection.
Clone()                 # Clone collection, fast O(1), uses Copy-on-write shadow cloning.
Clear()                 # Remove all items from collection
Release()               # Same as Clear() but optimized for cloned collections.

String()                # String representation
```

<!---------------------------------------------------------------------------
-- Stack
----------------------------------------------------------------------------->

## Stack

btree.Stack is a collection with the functionality of a stack - specifically, 
a LIFO (last-in, first-out) data structure.

### Operations

```py
Push(item)     # Insert item to top of stack.
Pop()          # Remove item from top stack.
Top()          # Get the top item in stack
PopFunc(cond)  # Remove item from top stack if cond returns true.
Contains(item) # Check if item exists

All()          # Iterate items in ascending order.      (iter.Seq[T])
Drain()        # Iterate and remove in ascending order. (iter.Seq[T])

Len()          # Get the number of items in collection.
Clone()        # Clone collection, fast O(1), uses Copy-on-write shadow cloning.
Clear()        # Remove all items from collection
Release()      # Same as Clear() but optimized for cloned collections.

String()       # String representation
```

```go
package main

import (
	"fmt"

	"github.com/tidwall/btree/v2"
)

func main() {
	// Create an stack of names
	var names btree.Stack[string]

	// Add some names
	names.Push("Andrea")
	names.Push("Tom")
	names.Push("Andy")
	names.Push("Jane")
	names.Push("Janet")
	names.Push("Steve")

	// Iterate over the stack and print each name
	for name := range names.All() {
		fmt.Printf("%s\n", name)
	}
	fmt.Printf("\n")

	// Pop a the top two names
	var name string
	name, _ = names.Pop()
	fmt.Printf("%s\n", name)
	name, _ = names.Pop()
	fmt.Printf("%s\n", name)
	fmt.Printf("\n")

	// Print the names again
	for name := range names.All() {
		fmt.Printf("%s\n", name)
	}
	fmt.Printf("\n")
}

// Output:
// Steve
// Janet
// Jane
// Andy
// Tom
// Andrea
//
// Steve
// Janet
//
// Jane
// Andy
// Tom
// Andrea
```

<!---------------------------------------------------------------------------
-- Stack
----------------------------------------------------------------------------->

## Queue

btree.Queue is a collection with the functionality of a queue - specifically, 
a FIFO (first-in, first-out) data structure.

### Operations

```py
Push(item)     # Insert item at the end of queue.
Pop()          # Remove the first item.
Front()        # Get the first item.
PopFunc(cond)  # Remove first item if cond function returns true.
Contains(item) # Checks if item exists

All()          # Iterate items in ascending order.      (iter.Seq[T])
Drain()        # Iterate and remove in ascending order. (iter.Seq[T])

Len()          # Get the number of items in collection.
Clone()        # Clone collection, fast O(1), uses Copy-on-write shadow cloning.
Clear()        # Remove all items from collection
Release()      # Same as Clear() but optimized for cloned collections.

String()       # String representation
```

### Example

```go
package main

import (
	"fmt"

	"github.com/tidwall/btree/v2"
)

func main() {
	// Create an queue of names
	var names btree.Queue[string]

	// Add some names
	names.Push("Andrea")
	names.Push("Tom")
	names.Push("Andy")
	names.Push("Jane")
	names.Push("Janet")
	names.Push("Steve")

	// Iterate over the array and print each name
	for name := range names.All() {
		fmt.Printf("%s\n", name)
	}
	fmt.Printf("\n")

	// Pop the first two names
	var name string
	name, _ = names.Pop()
	fmt.Printf("%s\n", name)
	name, _ = names.Pop()
	fmt.Printf("%s\n", name)
	fmt.Printf("\n")

	// Print the names again
	for name := range names.All() {
		fmt.Printf("%s\n", name)
	}
	fmt.Printf("\n")
}

// Output:
// Andrea
// Tom
// Andy
// Jane
// Janet
// Steve
//
// Andrea
// Tom
//
// Andy
// Jane
// Janet
// Steve
```

<!---------------------------------------------------------------------------
-- Deque
----------------------------------------------------------------------------->

## Deque

btree.Deque is a double-ended queue.

### Operations

```py
Push(item)         # Insert item at the end of queue.
PopFront()         # Remove the first item.
PopBack()          # Remove the last item.
Front()            # Get the first item.
Back( )            # Get the last item.
PopFrontFunc(cond) # Remove first item if cond returns true.
PopBackFunc(cond)  # Remove last item if cond returns true.
Contains(item)     # Checks if item exists

All()              # Iterate items in ascending order.       (iter.Seq[T])
Backward()         # Iterate items in desending order.       (iter.Seq[T])
Drain()            # Iterate and remove in ascending order.  (iter.Seq[T])
DrainBackward()    # Iterate and remove in descending order. (iter.Seq[T])

Len()              # Get the number of items in collection.
Clone()            # Clone collection, fast O(1), uses Copy-on-write shadow cloning.
Clear()            # Remove all items from collection
Release()          # Same as Clear() but optimized for cloned collections.

String()           # String representation
```

### Example

```go
package main

import (
	"fmt"

	"github.com/tidwall/btree/v2"
)

func main() {
	// Create an queue of names
	var names btree.Deque[string]

	// Add some names
	names.PushFront("Andrea")
	names.PushBack("Tom")
	names.PushFront("Andy")
	names.PushBack("Jane")
	names.PushFront("Janet")
	names.PushBack("Steve")

	// Iterate over the array and print each name
	for name := range names.All() {
		fmt.Printf("%s\n", name)
	}
	fmt.Printf("\n")

	// Pop the first two names
	var name string
	name, _ = names.PopFront()
	fmt.Printf("%s\n", name)
	name, _ = names.PopBack()
	fmt.Printf("%s\n", name)
	fmt.Printf("\n")

	// Print the names again
	for name := range names.All() {
		fmt.Printf("%s\n", name)
	}
	fmt.Printf("\n")
}

// Output:
// Janet
// Andy
// Andrea
// Tom
// Jane
// Steve
//
// Janet
// Steve
//
// Andy
// Andrea
// Tom
// Jane
```

<!---------------------------------------------------------------------------
-- Deque
----------------------------------------------------------------------------->

## Prique

btree.Prique is a priority queue collection that sorts items by largest to 
smallest. Data operations, such as Push() and Pop(), are O(log n).

This collection has support for duplicate items.
It also inherits the [Table](#table) collection, allowing for 
[Tagged struct keys](#tagged-struct-keys).

### Operations

```py
Push(item)     # Insert item into queue.
Pop()          # Remove the largest item.
Front()        # Get the largest item.
PopFunc(cond)  # Remove largest item if cond returns true.
Contains(item) # Checks if item exists

All()          # Iterate items in order of largest to smallest.      (iter.Seq[T])
Drain()        # Iterate and remove in order of largest to smallest. (iter.Seq[T])

Len()          # Get the number of items in collection.
Clone()        # Clone collection, fast O(1), uses Copy-on-write shadow cloning.
Clear()        # Remove all items from collection
Release()      # Same as Clear() but optimized for cloned collections.

String()       # String representation
```

### Example

```go
package main

import (
	"fmt"

	"github.com/tidwall/btree/v2"
)

func main() {

	type User struct {
		age  int `btree:"key"`
		name string
	}

	// Create an queue of users
	var users btree.Prique[User]

	// Add some names
	users.Push(User{27, "Andrea"})
	users.Push(User{54, "Tom"})
	users.Push(User{31, "Andy"})
	users.Push(User{43, "Jane"})
	users.Push(User{29, "Janet"})
	users.Push(User{62, "Steve"})
	users.Push(User{31, "Morton"})

	// Iterate over the array and print each name
	for user := range users.All() {
		fmt.Printf("%v %v\n", user.name, user.age)
	}
	fmt.Printf("\n")

	// Pop the first two names
	var user User
	user, _ = users.Pop()
	fmt.Printf("%v %v\n", user.name, user.age)
	user, _ = users.Pop()
	fmt.Printf("%v %v\n", user.name, user.age)
	fmt.Printf("\n")

	// Print the names again
	for user := range users.All() {
		fmt.Printf("%v %v\n", user.name, user.age)
	}
	fmt.Printf("\n")
}

// Output:
// Steve 62
// Tom 54
// Jane 43
// Morton 31
// Andy 31
// Janet 29
// Andrea 27
// 
// Steve 62
// Tom 54
// 
// Jane 43
// Morton 31
// Andy 31
// Janet 29
// Andrea 27
```

## Performance

The btree package has various optimizations that enhance performance over existing implementations.

- Branch node counting.
- Auto-detected search method.
- Seperate keys and values for maps.
- Last-first search algo. (fast bounds check, fewer conditions, fast bulk loads)
- Reference counted COW.

### Benchmarks

https://github.com/tidwall/btree-bench

- CPU: Ryzen 9 5950X 16-Core Processor
- Go: go version go1.26.0 linux/amd64
- Rust: rustc 1.95.0 (59807616e 2026-04-14)
- C: gcc (Ubuntu 11.4.0-1ubuntu1~22.04.3) 11.4.0
- C++: g++ (Ubuntu 11.4.0-1ubuntu1~22.04.3) 11.4.0

### Implementations

- [tidwall/btree](https://github.com/tidwall/btree) Go
- [google/btree](https://github.com/google/btree) Go
- [rust/BTreeMap](https://doc.rust-lang.org/std/collections/struct.BTreeMap.html) Rust
- [tidwall/btree.h](https://github.com/tidwall/btree.h) C
- [frozenca/btree](https://github.com/frozenca/BTree) C++

Benchmarking 1,000,000 items, 50 runs, taking the average result.

### int32 keys

```
tidwall/btree
insert(seq)      1,000,000 ops in   0.017 secs   16.5 ns/op   60,469,589 op/sec
insert(rand)     1,000,000 ops in   0.075 secs   74.7 ns/op   13,382,882 op/sec
get(seq)         1,000,000 ops in   0.029 secs   29.4 ns/op   34,041,293 op/sec
get(rand)        1,000,000 ops in   0.065 secs   64.9 ns/op   15,407,170 op/sec

google/btree
insert(seq)      1,000,000 ops in   0.070 secs   69.8 ns/op   14,321,709 op/sec
insert(rand)     1,000,000 ops in   0.153 secs  153.4 ns/op    6,518,280 op/sec
get(seq)         1,000,000 ops in   0.065 secs   64.6 ns/op   15,486,010 op/sec
get(rand)        1,000,000 ops in   0.155 secs  154.9 ns/op    6,454,916 op/sec

rust/btree
insert(seq)      1,000,000 ops in   0.051 secs   51.0 ns/op   19,624,389 op/sec
insert(rand)     1,000,000 ops in   0.098 secs   98.2 ns/op   10,187,241 op/sec
get(seq)         1,000,000 ops in   0.033 secs   32.6 ns/op   30,650,401 op/sec
get(rand)        1,000,000 ops in   0.097 secs   97.0 ns/op   10,308,321 op/sec

tidwall/btree.h
insert(seq)      1,000,000 ops in   0.053 secs   52.6 ns/op   19,011,130 op/sec
insert(rand)     1,000,000 ops in   0.076 secs   75.8 ns/op   13,186,500 op/sec
get(seq)         1,000,000 ops in   0.033 secs   33.0 ns/op   30,264,262 op/sec
get(rand)        1,000,000 ops in   0.069 secs   68.9 ns/op   14,524,288 op/sec

frozenca/btree
insert(seq)      1,000,000 ops in   0.093 secs   92.7 ns/op   10,782,702 op/sec
insert(rand)     1,000,000 ops in   0.081 secs   81.5 ns/op   12,275,940 op/sec
get(seq)         1,000,000 ops in   0.044 secs   44.2 ns/op   22,636,693 op/sec
get(rand)        1,000,000 ops in   0.079 secs   79.1 ns/op   12,638,367 op/sec
```

### uint64 keys

```
tidwall/btree
insert(seq)      1,000,000 ops in   0.018 secs   18.1 ns/op   55,277,882 op/sec
insert(rand)     1,000,000 ops in   0.084 secs   84.1 ns/op   11,897,138 op/sec
get(seq)         1,000,000 ops in   0.030 secs   30.2 ns/op   33,057,894 op/sec
get(rand)        1,000,000 ops in   0.074 secs   73.6 ns/op   13,591,580 op/sec

google/btree
insert(seq)      1,000,000 ops in   0.077 secs   76.8 ns/op   13,028,686 op/sec
insert(rand)     1,000,000 ops in   0.173 secs  172.9 ns/op    5,784,271 op/sec
get(seq)         1,000,000 ops in   0.062 secs   61.9 ns/op   16,165,628 op/sec
get(rand)        1,000,000 ops in   0.166 secs  166.4 ns/op    6,008,125 op/sec

rust/btree
insert(seq)      1,000,000 ops in   0.044 secs   43.6 ns/op   22,936,305 op/sec
insert(rand)     1,000,000 ops in   0.105 secs  105.2 ns/op    9,502,632 op/sec
get(seq)         1,000,000 ops in   0.034 secs   33.9 ns/op   29,509,841 op/sec
get(rand)        1,000,000 ops in   0.107 secs  106.8 ns/op    9,362,769 op/sec

tidwall/btree.h
insert(seq)      1,000,000 ops in   0.054 secs   53.7 ns/op   18,605,963 op/sec
insert(rand)     1,000,000 ops in   0.081 secs   80.6 ns/op   12,406,050 op/sec
get(seq)         1,000,000 ops in   0.033 secs   32.6 ns/op   30,657,821 op/sec
get(rand)        1,000,000 ops in   0.075 secs   75.4 ns/op   13,269,668 op/sec

frozenca/btree
insert(seq)      1,000,000 ops in   0.094 secs   93.7 ns/op   10,668,965 op/sec
insert(rand)     1,000,000 ops in   0.094 secs   93.6 ns/op   10,688,826 op/sec
get(seq)         1,000,000 ops in   0.044 secs   43.5 ns/op   22,964,359 op/sec
get(rand)        1,000,000 ops in   0.087 secs   87.0 ns/op   11,497,710 op/sec
```

### string keys

```
tidwall/btree
insert(seq)      1,000,000 ops in   0.078 secs    78.4 ns/op  12,758,865 op/sec
insert(rand)     1,000,000 ops in   0.397 secs   397.3 ns/op   2,516,762 op/sec
get(seq)         1,000,000 ops in   0.140 secs   140.2 ns/op   7,130,353 op/sec
get(rand)        1,000,000 ops in   0.450 secs   449.5 ns/op   2,224,584 op/sec

google/btree
insert(seq)      1,000,000 ops in   0.191 secs  191.0 ns/op    5,234,886 op/sec
insert(rand)     1,000,000 ops in   0.437 secs  437.3 ns/op    2,286,980 op/sec
get(seq)         1,000,000 ops in   0.146 secs  145.9 ns/op    6,855,693 op/sec
get(rand)        1,000,000 ops in   0.487 secs  487.0 ns/op    2,053,473 op/sec

rust/btree
insert(seq)      1,000,000 ops in   0.250 secs  250.3 ns/op    3,995,141 op/sec
insert(rand)     1,000,000 ops in   0.510 secs  510.3 ns/op    1,959,504 op/sec
get(seq)         1,000,000 ops in   0.218 secs  218.3 ns/op    4,581,355 op/sec
get(rand)        1,000,000 ops in   0.591 secs  591.0 ns/op    1,692,138 op/sec

tidwall/btree.h
insert(seq)      1,000,000 ops in   0.392 secs  392.3 ns/op    2,549,279 op/sec
insert(rand)     1,000,000 ops in   0.400 secs  400.5 ns/op    2,496,899 op/sec
get(seq)         1,000,000 ops in   0.466 secs  466.3 ns/op    2,144,526 op/sec
get(rand)        1,000,000 ops in   0.478 secs  477.6 ns/op    2,093,912 op/sec

frozenca/btree
insert(seq)      1,000,000 ops in   0.636 secs  636.3 ns/op    1,571,476 op/sec
insert(rand)     1,000,000 ops in   0.633 secs  632.7 ns/op    1,580,596 op/sec
get(seq)         1,000,000 ops in   0.376 secs  376.5 ns/op    2,656,345 op/sec
get(rand)        1,000,000 ops in   0.618 secs  618.4 ns/op    1,617,120 op/sec
```
