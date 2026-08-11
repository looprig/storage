package leasesexample_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func Example_epochFencing() {
	ctx := context.Background()
	leaser := memstore.New().Leaser
	first, err := leaser.Acquire(ctx, "workers/indexer")
	if err != nil {
		panic(err)
	}

	_, err = leaser.Acquire(ctx, "workers/indexer")
	var held *storage.LeaseHeldError
	fmt.Printf("held=%t epoch=%d\n", errors.As(err, &held), held.HolderEpoch)

	if err := first.Release(ctx); err != nil {
		panic(err)
	}
	_, lost := <-first.Lost()
	second, err := leaser.Acquire(ctx, "workers/indexer")
	if err != nil {
		panic(err)
	}
	defer second.Release(ctx)
	fmt.Printf("lost=%t next-epoch=%d\n", !lost, second.Epoch())

	// Output:
	// held=true epoch=1
	// lost=true next-epoch=2
}
