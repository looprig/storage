package blobsexample_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func Example_immutableContent() {
	ctx := context.Background()
	blobs := memstore.New().Blobs
	const key = "snapshots/sha256-demo"

	if err := blobs.Put(ctx, key, bytes.NewBufferString("workspace")); err != nil {
		panic(err)
	}
	// Repeating byte-identical content is an idempotent success.
	if err := blobs.Put(ctx, key, bytes.NewBufferString("workspace")); err != nil {
		panic(err)
	}
	err := blobs.Put(ctx, key, bytes.NewBufferString("different"))
	var conflict *storage.BlobConflictError
	fmt.Println("conflict:", errors.As(err, &conflict))

	r, err := blobs.Get(ctx, key)
	if err != nil {
		panic(err)
	}
	defer r.Close()
	content, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	fmt.Println("original:", string(content))

	// Output:
	// conflict: true
	// original: workspace
}
