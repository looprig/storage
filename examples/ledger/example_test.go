package ledgerexample_test

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/looprig/storage/memstore"
)

func Example_boundedCursor() {
	ctx := context.Background()
	ledger := memstore.New().Ledger

	if err := ledger.Append(ctx, "events/orders", 0, []byte("created")); err != nil {
		panic(err)
	}
	cursor, err := ledger.Read(ctx, "events/orders", 1)
	if err != nil {
		panic(err)
	}
	defer cursor.Close()

	// A cursor is bounded at Read time, so this later event is not tailed.
	if err := ledger.Append(ctx, "events/orders", 1, []byte("paid")); err != nil {
		panic(err)
	}
	first, err := cursor.Next(ctx)
	if err != nil {
		panic(err)
	}
	_, drained := cursor.Next(ctx)
	fmt.Printf("snapshot: %d %s drained=%t\n", first.Seq, first.Payload, errors.Is(drained, io.EOF))

	fresh, err := ledger.Read(ctx, "events/orders", 2)
	if err != nil {
		panic(err)
	}
	defer fresh.Close()
	second, err := fresh.Next(ctx)
	if err != nil {
		panic(err)
	}
	tip, err := ledger.Tip(ctx, "events/orders")
	if err != nil {
		panic(err)
	}
	fmt.Printf("fresh: %d %s tip=%d\n", second.Seq, second.Payload, tip)

	// Output:
	// snapshot: 1 created drained=true
	// fresh: 2 paid tip=2
}
