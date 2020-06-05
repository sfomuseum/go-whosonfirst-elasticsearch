package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	es "github.com/elastic/go-elasticsearch/v7"
	// "github.com/elastic/go-elasticsearch/v7/estransport"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/sfomuseum/go-flags/flagset"
	"github.com/sfomuseum/go-flags/lookup"
	"github.com/tidwall/gjson"
	"github.com/whosonfirst/go-whosonfirst-index"
	"io"
	"io/ioutil"
	"log"
	// "os"
	"runtime"
	"strconv"
	"time"
)

const FLAG_ES_ENDPOINT string = "elasticsearch-endpoint"
const FLAG_ES_INDEX string = "elasticsearch-index"
const FLAG_INDEXER_URI string = "indexer-uri"
const FLAG_INDEX_ALT string = "index-alt-files"
const FLAG_INDEX_PROPS string = "index-only-properties"
const FLAG_WORKERS string = "workers"

type BulkToolChannels struct {
	Done  chan bool
	Error chan error
}

func NewBulkToolFlagSet(ctx context.Context) (*flag.FlagSet, error) {

	fs := flagset.NewFlagSet("bulk")

	fs.String(FLAG_ES_ENDPOINT, "http://localhost:9200", "...")
	fs.String(FLAG_ES_INDEX, "millsfield", "...")
	fs.String(FLAG_INDEXER_URI, "repo://", "...")
	fs.Bool(FLAG_INDEX_ALT, false, "...")
	fs.Bool(FLAG_INDEX_PROPS, false, "...")
	fs.Int(FLAG_WORKERS, runtime.NumCPU(), "...")

	// debug := fs.Bool("debug", false, "...")

	return fs, nil
}

func RunBulkToolWithFlagSetAndChannels(ctx context.Context, fs *flag.FlagSet, channels *BulkToolChannels) (*esutil.BulkIndexerStats, error) {
     return nil, errors.New("Not implemented")
}

func RunBulkToolWithFlagSet(ctx context.Context, fs *flag.FlagSet) (*esutil.BulkIndexerStats, error) {

	es_endpoint, err := lookup.StringVar(fs, FLAG_ES_ENDPOINT)

	if err != nil {
		return nil, err
	}

	es_index, err := lookup.StringVar(fs, FLAG_ES_INDEX)

	if err != nil {
		return nil, err
	}

	idx_uri, err := lookup.StringVar(fs, FLAG_INDEXER_URI)

	if err != nil {
		return nil, err
	}

	workers, err := lookup.IntVar(fs, FLAG_WORKERS)

	if err != nil {
		return nil, err
	}

	index_alt, err := lookup.BoolVar(fs, FLAG_INDEX_ALT)

	if err != nil {
		return nil, err
	}

	index_only_props, err := lookup.BoolVar(fs, FLAG_INDEX_PROPS)

	if err != nil {
		return nil, err
	}

	retry := backoff.NewExponentialBackOff()

	es_cfg := es.Config{
		Addresses: []string{es_endpoint},

		RetryOnStatus: []int{502, 503, 504, 429},
		RetryBackoff: func(i int) time.Duration {
			if i == 1 {
				retry.Reset()
			}
			return retry.NextBackOff()
		},
		MaxRetries: 5,
	}

	/*

		if debug {

			es_logger := &estransport.ColorLogger{
				Output:             os.Stdout,
				EnableRequestBody:  true,
				EnableResponseBody: true,
			}

			es_cfg.Logger = es_logger
		}

	*/

	es_client, err := es.NewClient(es_cfg)

	if err != nil {
		return nil, err
	}

	_, err = es_client.Indices.Create(es_index)

	if err != nil {
		return nil, err
	}

	// https://github.com/elastic/go-elasticsearch/blob/master/_examples/bulk/indexer.go

	bi_cfg := esutil.BulkIndexerConfig{
		Index:         es_index,
		Client:        es_client,
		NumWorkers:    workers,
		FlushInterval: 30 * time.Second,
	}

	bi, err := esutil.NewBulkIndexer(bi_cfg)

	if err != nil {
		return nil, err
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

		wof_id := id_rsp.Int()
		doc_id := strconv.FormatInt(wof_id, 10)

		alt_rsp := gjson.GetBytes(body, "properties.src:alt_label")

		if alt_rsp.Exists() {

			if !index_alt {
				return nil
			}

			doc_id = fmt.Sprintf("%s-%s", doc_id, alt_rsp.String())
		}

		// START OF manipulate body here...

		if index_only_props {

			props_rsp := gjson.GetBytes(body, "properties")

			if !props_rsp.Exists() {
				msg := fmt.Sprintf("%s is missing properties")
				return errors.New(msg)
			}

			props_body, err := json.Marshal(props_rsp.Value())

			if err != nil {
				msg := fmt.Sprintf("Failed to encode properties for %s, %v", path, err)
				return errors.New(msg)
			}

			body = props_body
		}

		// END OF manipulate body here...

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

		return nil
	}

	i, err := index.NewIndexer(idx_uri, cb)

	if err != nil {
		return nil, err
	}

	paths := fs.Args()

	err = i.Index(ctx, paths...)

	if err != nil {
		return nil, err
	}

	err = bi.Close(ctx)

	if err != nil {
		return nil, err
	}

	stats := bi.Stats()
	return &stats, nil
}
