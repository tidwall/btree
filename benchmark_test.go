// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
package btree

import (
	"testing"
)

func BenchmarkIterAndSeek(b *testing.B) {
	tree := NewBTreeGOptions(func(a, b int) bool {
		return a < b
	}, Options{
		Degree: 4,
	})
	for i := 0; i < 10<<20; i++ {
		tree.Set(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := tree.Iter()
		iter.Seek(1)
		iter.Release()
	}
}
