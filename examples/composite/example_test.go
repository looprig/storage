package compositeexample_test

import (
	"context"
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
	legacyPrimitives := store.Ledger != nil && store.Leaser != nil && store.KV != nil && store.Blobs != nil
	fmt.Printf("legacy primitives=%t ordered-index-nil=%t\n", legacyPrimitives, store.OrderedIndex == nil)

	_, err = storage.NewComposite(memory.Ledger, nil, memory.KV, memory.Blobs)
	var incomplete *storage.IncompleteCompositeError
	fmt.Println("missing leaser:", errors.As(err, &incomplete), incomplete.Missing)

	// Output:
	// legacy primitives=true ordered-index-nil=true
	// missing leaser: true [Leaser]
}

func ExampleNewCompositeWithOrderedIndex() {
	ctx := context.Background()
	memory := memstore.New()
	store, err := storage.NewCompositeWithOrderedIndex(memory.Ledger, memory.Leaser, memory.KV, memory.Blobs, memory.OrderedIndex)
	if err != nil {
		panic(err)
	}

	record, created, err := store.OrderedIndex.Create(ctx,
		storage.OrderedID{Namespace: "sessions", OrderingScope: "acceptance", StableKey: "session-42"},
		"recent", []byte("queued"), storage.Rank{Ranked: true, Value: 10}, storage.Due{State: storage.NotDue})
	if err != nil {
		panic(err)
	}

	complete := store.Ledger != nil && store.Leaser != nil && store.KV != nil && store.Blobs != nil && store.OrderedIndex != nil
	fmt.Printf("complete=%t created=%t order=%d\n", complete, created, record.Order)

	// Output:
	// complete=true created=true order=1
}
