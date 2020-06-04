package main

import (
	_ "github.com/whosonfirst/go-whosonfirst-index/fs"
)

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	es "github.com/elastic/go-elasticsearch/v7"
	_ "github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/estransport"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/tidwall/gjson"
	"github.com/whosonfirst/go-whosonfirst-index"
	"io"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"
)

func main() {

	es_endpoint := flag.String("elasticsearch-endpoint", "http://localhost:9200", "...")
	es_index := flag.String("elasticsearch-index", "millsfield", "...")

	idx_uri := flag.String("indexer-uri", "repo://", "...")

	workers := flag.Int("workers", runtime.NumCPU(), "...")
	debug := flag.Bool("debug", false, "...")

	flag.Parse()

	ctx := context.Background()

	retryBackoff := backoff.NewExponentialBackOff()
	log.Println(retryBackoff)

	es_cfg := es.Config{
		Addresses: []string{*es_endpoint},

		RetryOnStatus: []int{502, 503, 504, 429},
		RetryBackoff: func(i int) time.Duration {
			if i == 1 {
				retryBackoff.Reset()
			}
			return retryBackoff.NextBackOff()
		},
		MaxRetries: 5,
	}

	if *debug {

		es_logger := &estransport.ColorLogger{
			Output:             os.Stdout,
			EnableRequestBody:  true,
			EnableResponseBody: true,
		}

		es_cfg.Logger = es_logger
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

	bi_cfg := esutil.BulkIndexerConfig{
		Index:         *es_index,
		Client:        es_client,
		NumWorkers:    *workers,
		FlushInterval: 30 * time.Second,
	}

	bi, err := esutil.NewBulkIndexer(bi_cfg)

	if err != nil {
		log.Fatalf("Error creating the indexer: %s", err)
	}

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

		// FIX ME : CHECK FOR ALT FILES - ADJUST doc_id ACCORDINGLY

		wof_id := id_rsp.Int()
		doc_id := strconv.FormatInt(wof_id, 10)

		// manipulate body here...

		var f interface{}
		err = json.Unmarshal(body, &f)

		if err != nil {
			msg := fmt.Sprintf("Failed to unmarshal %s, %v", path, err)
			return errors.New(msg)
		}

		enc_f, err := json.Marshal(f)

		if err != nil {
			msg := fmt.Sprintf("Failed to marshal %s, %v", path, err)
			return errors.New(msg)
		}

		bulk_item := esutil.BulkIndexerItem{
			Action:     "index",
			DocumentID: doc_id,
			Body:       bytes.NewReader(enc_f),

			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
				// log.Printf("Indexed %s\n", path)
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

		if err != nil {
			log.Printf("Failed to schedule %s, %v", path, err)
			return nil
		}

		log.Printf("Schedule to index %s\n", path)
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

	err = bi.Close(ctx)

	if err != nil {
		log.Fatal(err)
	}

	stats := bi.Stats()
	enc, _ := json.Marshal(stats)
	log.Println(string(enc))
}
