package main

import (
	_ "github.com/whosonfirst/go-whosonfirst-index/fs"
)

import (
	"context"
	"flag"
	es_client "github.com/sfomuseum/go-sfomuseum-elasticsearch/client"
	es_index "github.com/sfomuseum/go-sfomuseum-elasticsearch/index"
	"github.com/whosonfirst/go-whosonfirst-index"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"io"
	"log"
	"strconv"
)

func main() {

	es_endpoint := flag.String("elasticsearch-endpoint", "http://localhost:9200", "...")
	es_index_name := flag.String("elasticsearch-index", "sfomuseum", "...")

	idx_uri := flag.String("indexer-uri", "repo://", "...")

	flag.Parse()

	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cl, err := es_client.NewClientWithEndpoint(*es_endpoint)

	if err != nil {
		log.Fatalf("Failed to create ES client, %v", err)
	}

	_, err = cl.Indices.Create(*es_index_name)

	if err != nil {
		log.Fatalf("Failed to create ES index, %v", err)
	}

	done := false

	cb := func(ctx context.Context, fh io.Reader, args ...interface{}) error {

		if done {
			return nil
		}

		path, err := index.PathForContext(ctx)

		if err != nil {
			return err
		}

		id, uri_args, err := uri.ParseURI(path)

		if err != nil {
			return err
		}

		if uri_args.IsAlternate {
			return nil
		}

		str_id := strconv.FormatInt(id, 10)

		return es_index.IndexDocumentWithReader(ctx, cl, "flights", str_id, fh)
	}

	i, err := index.NewIndexer(*idx_uri, cb)

	if err != nil {
		log.Fatal(err)
	}

	paths := flag.Args()

	err = i.Index(ctx, paths...)

	if err != nil {
		log.Fatal(err)
	}

}
