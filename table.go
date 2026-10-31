// https://github.com/tidwall/btree
//
// Copyright 2026 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
//
// B-tree based collections for go
package btree // import "github.com/tidwall/btree/v2"

import (
	"cmp"
	"fmt"
	"iter"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
	"unsafe"
)

////////////////////////////////////////////////////////////////////////////////
// Table
////////////////////////////////////////////////////////////////////////////////

// Table is a B-tree backed in-memory collection for general purpose items.
type Table[T any] struct {
	tree tree[omit, T]
}

// TableOptions for NewTableOptions()
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

func (b *Table[T]) String() string {
	return fmt.Sprintf("table%v", slices.Collect(b.All()))
}

func (b *Table[T]) Lookup(key T) (T, bool) {
	return b.tree.Lookup(omit{}, key)
}

func (b *Table[T]) LookupMut(key T) (T, bool) {
	return b.tree.LookupMut(omit{}, key)
}

func (b *Table[T]) Get(key T) T {
	return b.tree.Get(omit{}, key)
}

func (b *Table[T]) GetMut(key T) T {
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

func (b *Table[T]) PopFrontFunc(cond func(item T) bool) (T, bool) {
	if cond == nil {
		return b.PopFront()
	}
	_, value, ok := b.tree.PopFrontFunc(func(_ omit, item T) bool {
		return cond(item)
	})
	return value, ok
}

func (b *Table[T]) PopBackFunc(cond func(item T) bool) (T, bool) {
	if cond == nil {
		return b.PopBack()
	}
	_, value, ok := b.tree.PopBackFunc(func(_ omit, item T) bool {
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

func (b *Table[T]) LookupAt(index int) (T, bool) {
	_, value, ok := b.tree.LookupAt(index)
	return value, ok
}

func (b *Table[T]) LookupAtMut(index int) (T, bool) {
	_, value, ok := b.tree.LookupAtMut(index)
	return value, ok
}

func (b *Table[T]) GetAt(index int) T {
	_, value := b.tree.GetAt(index)
	return value
}

func (b *Table[T]) GetAtMut(index int) T {
	_, value := b.tree.GetAtMut(index)
	return value
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

func (b *Table[T]) DrainAtOptions(index, count int, opts DrainOptions,
) Extract[T] {
	return sliceV(b.tree.DrainAt(index, count, opts))
}

func (b *Table[T]) DrainOptions(min, max T, opts DrainOptions) Extract[T] {
	return sliceV(b.tree.Drain(omit{}, min, omit{}, max, opts))
}

func (b *Table[T]) DrainAt(index, count int) Extract[T] {
	return b.DrainAtOptions(index, count, DrainOptions{})
}

func (b *Table[T]) Drain(min, max T) Extract[T] {
	return b.DrainOptions(min, max, DrainOptions{})
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

func (b *Table[T]) Clone() *Table[T] {
	b2 := new(Table[T])
	*b2 = *b
	b.tree.CloneInto(&b2.tree)
	return b2
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
		var idx int
		var field cfield
		var collate ccollate
		var dir = casc
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
