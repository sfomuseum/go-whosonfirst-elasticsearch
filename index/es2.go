package index

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/sfomuseum/go-flags/lookup"
	"github.com/sfomuseum/go-whosonfirst-elasticsearch/document"
	"github.com/tidwall/gjson"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/iterator"
	es "gopkg.in/olivere/elastic.v3"
	"io"
	"log"
	"strconv"
	"time"
)

// RunBulkIndexerWithFlagSet will "bulk" index a set of Who's On First documents with configuration details defined by `fs`.
func RunES2BulkIndexerWithFlagSet(ctx context.Context, fs *flag.FlagSet) (*es.BulkProcessorStats, error) {

	es_endpoint, err := lookup.StringVar(fs, FLAG_ES_ENDPOINT)

	if err != nil {
		return nil, err
	}

	es_index, err := lookup.StringVar(fs, FLAG_ES_INDEX)

	if err != nil {
		return nil, err
	}

	iter_uri, err := lookup.StringVar(fs, FLAG_ITERATOR_URI)

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

	index_spelunker_v1, err := lookup.BoolVar(fs, FLAG_INDEX_SPELUNKER_V1)

	if err != nil {
		return nil, err
	}

	append_spelunker_v1, err := lookup.BoolVar(fs, FLAG_APPEND_SPELUNKER_V1)

	if err != nil {
		return nil, err
	}

	if index_spelunker_v1 {

		if index_only_props {
			msg := fmt.Sprintf("-%s can not be used when -%s is enabled", FLAG_INDEX_PROPS, FLAG_INDEX_SPELUNKER_V1)
			return nil, errors.New(msg)
		}

		if append_spelunker_v1 {
			msg := fmt.Sprintf("-%s can not be used when -%s is enabled", FLAG_APPEND_SPELUNKER_V1, FLAG_INDEX_SPELUNKER_V1)
			return nil, errors.New(msg)
		}
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

	iter_cb := func(ctx context.Context, path string, fh io.ReadSeeker, args ...interface{}) error {

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

		prepare_funcs := make([]document.PrepareDocumentFunc, 0)

		if index_spelunker_v1 {
			prepare_funcs = append(prepare_funcs, document.PrepareSpelunkerV1Document)
		}

		if index_only_props {
			prepare_funcs = append(prepare_funcs, document.ExtractProperties)
		}

		if append_spelunker_v1 {
			prepare_funcs = append(prepare_funcs, document.AppendSpelunkerV1Properties)
		}

		for _, f := range prepare_funcs {

			new_body, err := f(ctx, body)

			if err != nil {
				return err
			}

			body = new_body
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

		// log.Println(string(enc_f))

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

	iter, err := iterator.NewIterator(ctx, iter_uri, iter_cb)

	if err != nil {
		return nil, err
	}

	paths := fs.Args()

	t1 := time.Now()

	err = iter.IterateURIs(ctx, paths...)

	if err != nil {
		return nil, err
	}

	err = bi.Close(ctx)

	if err != nil {
		return nil, err
	}

	log.Printf("Processed %d files in %v\n", iter.Seen, time.Since(t1))

	stats := bi.Stats()
	return &stats, nil
}
