package main

import (
	_ "github.com/whosonfirst/go-whosonfirst-index/fs"
	_ "github.com/whosonfirst/go-whosonfirst-index-git"
)

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/sfomuseum/go-flags/flagset"
	"github.com/sfomuseum/go-whosonfirst-elasticsearch/index"
	"log"
)

func main() {

	ctx := context.Background()

	fs, err := index.NewBulkIndexerFlagSet(ctx)

	if err != nil {
		log.Fatalf("Failed to create new flagset, %v", err)
	}

	flagset.Parse(fs)

	stats, err := index.RunBulkIndexerWithFlagSet(ctx, fs)

	if err != nil {
		log.Fatalf("Failed to run bulk tool, %v", err)
	}

	enc_stats, err := json.Marshal(stats)

	if err != nil {
		log.Fatalf("Failed to marshal stats, %v", err)
	}

	fmt.Println(string(enc_stats))
}
