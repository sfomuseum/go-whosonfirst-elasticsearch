package main

import (
	_ "github.com/whosonfirst/go-whosonfirst-index/fs"
)

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	es "github.com/elastic/go-elasticsearch/v7"
	_ "github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/tidwall/gjson"
	"github.com/whosonfirst/go-whosonfirst-index"
	"io"
	"io/ioutil"
	"log"
	"runtime"
	"strconv"
	"time"
)

func main() {

	es_endpoint := flag.String("elasticsearch-endpoint", "http://localhost:9200", "...")
	es_index := flag.String("elasticsearch-index", "millsfield", "...")

	idx_uri := flag.String("indexer-uri", "repo://", "...")

	workers := flag.Int("workers", runtime.NumCPU(), "...")
	flush_bytes := flag.Int("flush", 5e+6, "Flush threshold in bytes")

	// bulk := flag.Bool("bulk", false, "...")

	flag.Parse()

	ctx := context.Background()

	retryBackoff := backoff.NewExponentialBackOff()

	es_cfg := es.Config{
		Addresses:     []string{*es_endpoint},
		RetryOnStatus: []int{502, 503, 504, 429},
		RetryBackoff: func(i int) time.Duration {
			if i == 1 {
				retryBackoff.Reset()
			}
			return retryBackoff.NextBackOff()
		},
		MaxRetries: 5,
	}

	es_client, err := es.NewClient(es_cfg)

	if err != nil {
		log.Fatalf("Failed to create ES client, %v", err)
	}

	_, err = es_client.Indices.Create(*es_index)

	if err != nil {
		log.Fatalf("Failed to create ES index, %v", err)
	}

	// https://github.com/elastic/go-elasticsearch/blob/master/_examples/bulk/indexer.go

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         *es_index,
		Client:        es_client,
		NumWorkers:    *workers,
		FlushBytes:    int(*flush_bytes),
		FlushInterval: 30 * time.Second,
	})

	if err != nil {
		log.Fatalf("Error creating the indexer: %s", err)
	}

	defer bi.Close(ctx)

	/*
	index_record := func(ctx context.Context, doc_id string, fh io.Reader) error {

		req := esapi.IndexRequest{
			Index:      *es_index,
			DocumentID: doc_id,
			Body:       fh,
			Refresh:    "true",
		}

		rsp, err := req.Do(ctx, es_client)

		if err != nil {
			return err
		}

		defer rsp.Body.Close()

		if rsp.IsError() {
			body, _ := ioutil.ReadAll(rsp.Body)
			return errors.New(string(body))
		}

		return nil
	}
	*/

	cb := func(ctx context.Context, fh io.Reader, args ...interface{}) error {

		path, err := index.PathForContext(ctx)

		if err != nil {
			return err
		}

		body, err := ioutil.ReadAll(fh)

		if err != nil {
			return err
		}

		id_rsp := gjson.GetBytes(body, "properties.wof:id")

		if !id_rsp.Exists() {
			msg := fmt.Sprintf("%s is missing properties.wof:id", path)
			return errors.New(msg)
		}

		wof_id := id_rsp.Int()
		doc_id := strconv.FormatInt(wof_id, 10)

		// manipulate body here...

		bulk_item := esutil.BulkIndexerItem{
			Action:     "index",
			DocumentID: doc_id,
			Body:       bytes.NewReader(body),

			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
				log.Printf("Indexed %s\n", path)
			},

			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				if err != nil {
					log.Printf("ERROR: Failed to index %s, %s", path, err)
				} else {
					log.Printf("ERROR: Failed to index %s, %s: %s", path, res.Error.Type, res.Error.Reason)
				}
			},
		}

		err = bi.Add(ctx, bulk_item)

		// err = index_record(ctx, doc_id, br)

		if err != nil {
			log.Printf("Failed to index %s, %v", path, err)
		}

		return nil
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
