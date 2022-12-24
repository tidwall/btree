package btree

// Generic BTree
//
// Deprecated: use BTreeG
type Generic[T any] struct {
	*BTreeG[T]
}

// NewGeneric returns a generic BTree
//
// Deprecated: use NewBTreeG
func NewGeneric[T any](less func(a, b T) bool) *Generic[T] {
	return &Generic[T]{NewBTreeGOptions(less, Options{})}
}

// NewGenericOptions returns a generic BTree
//
// Deprecated: use NewBTreeGOptions
func NewGenericOptions[T any](less func(a, b T) bool, opts Options,
) *Generic[T] {
	return &Generic[T]{NewBTreeGOptions(less, opts)}
}

func (tr *Generic[T]) Copy() *Generic[T] {
	return &Generic[T]{tr.BTreeG.Copy()}
}
