package compositeexample_test

import (
	"errors"
	"fmt"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func ExampleNewComposite() {
	memory := memstore.New()
	store, err := storage.NewComposite(memory.Ledger, memory.Leaser, memory.KV, memory.Blobs)
	if err != nil {
		panic(err)
	}
	fmt.Println("all primitives:", store.Ledger != nil && store.Leaser != nil && store.KV != nil && store.Blobs != nil)

	_, err = storage.NewComposite(memory.Ledger, nil, memory.KV, memory.Blobs)
	var incomplete *storage.IncompleteCompositeError
	fmt.Println("missing leaser:", errors.As(err, &incomplete), incomplete.Missing)

	// Output:
	// all primitives: true
	// missing leaser: true [Leaser]
}
