package kvexample_test

import (
	"context"
	"fmt"

	"github.com/looprig/storage/memstore"
)

func Example_revisionCAS() {
	ctx := context.Background()
	kv := memstore.New().KV

	rev1, err := kv.Put(ctx, "sessions/alpha", 0, []byte("queued"))
	if err != nil {
		panic(err)
	}
	rev2, err := kv.Put(ctx, "sessions/alpha", rev1, []byte("running"))
	if err != nil {
		panic(err)
	}
	if _, err := kv.Put(ctx, "sessions/beta", 0, []byte("queued")); err != nil {
		panic(err)
	}
	value, current, err := kv.Get(ctx, "sessions/alpha")
	if err != nil {
		panic(err)
	}
	keys, err := kv.Keys(ctx, "sessions/")
	if err != nil {
		panic(err)
	}
	fmt.Printf("revisions=%d,%d current=%d value=%s\n", rev1, rev2, current, value)
	fmt.Println(keys)

	// Output:
	// revisions=1,2 current=2 value=running
	// [sessions/alpha sessions/beta]
}
