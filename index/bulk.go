package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	es "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/sfomuseum/go-flags/flagset"
	"github.com/sfomuseum/go-flags/lookup"
	"github.com/sfomuseum/go-whosonfirst-elasticsearch/document"
	"github.com/tidwall/gjson"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/emitter"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/iterator"
	"io"
	"log"
	"strconv"
	"strings"
	"time"
)

const FLAG_ES_ENDPOINT string = "elasticsearch-endpoint"
const FLAG_ES_INDEX string = "elasticsearch-index"
const FLAG_ITERATOR_URI string = "iterator-uri"
const FLAG_INDEX_ALT string = "index-alt-files"
const FLAG_INDEX_PROPS string = "index-only-properties"
const FLAG_INDEX_SPELUNKER_V1 string = "index-spelunker-v1"
const FLAG_APPEND_SPELUNKER_V1 string = "append-spelunker-v1-properties"
const FLAG_WORKERS string = "workers"

// type RunBulkIndexerOptions contains runtime configurations for bulk indexing
type RunBulkIndexerOptions struct {
	// BulkIndexer is a `esutil.BulkIndexer` instance
	BulkIndexer esutil.BulkIndexer
	// PrepareFuncs are one or more `document.PrepareDocumentFunc` used to transform a document before indexing
	PrepareFuncs []document.PrepareDocumentFunc
	// IteratorURI is a valid `whosonfirst/go-whosonfirst-iterate/v2` URI string.
	IteratorURI string
	// IteratorPaths are one or more valid `whosonfirst/go-whosonfirst-iterate/v2` paths to iterate over
	IteratorPaths []string
	// IndexAltFiles is a boolean value indicating whether or not to index "alternate geometry" files
	IndexAltFiles bool
}

// NewBulkIndexerFlagSet creates a new `flag.FlagSet` instance with command-line flags required by the `es-whosonfirst-index` tool.
func NewBulkIndexerFlagSet(ctx context.Context) (*flag.FlagSet, error) {

	fs := flagset.NewFlagSet("bulk")

	valid_schemes := strings.Join(emitter.Schemes(), ",")
	iterator_desc := fmt.Sprintf("A valid whosonfirst/go-whosonfirst-iterator/emitter URI. Supported emitter URI schemes are: %s", valid_schemes)

	fs.String(FLAG_ES_ENDPOINT, "http://localhost:9200", "A fully-qualified Elasticsearch endpoint.")
	fs.String(FLAG_ES_INDEX, "millsfield", "A valid Elasticsearch index.")
	fs.String(FLAG_ITERATOR_URI, "repo://", iterator_desc)
	fs.Bool(FLAG_INDEX_ALT, false, "Index alternate geometries.")
	fs.Bool(FLAG_INDEX_PROPS, false, "Only index GeoJSON Feature properties (not geometries).")
	fs.Bool(FLAG_INDEX_SPELUNKER_V1, false, "Index GeoJSON Feature properties inclusive of auto-generated Whos On First Spelunker properties.")
	fs.Bool(FLAG_APPEND_SPELUNKER_V1, false, "Append and index auto-generated Whos On First Spelunker properties.")
	fs.Int(FLAG_WORKERS, 0, "The number of concurrent workers to index data using. Default is the value of runtime.NumCPU().")

	// debug := fs.Bool("debug", false, "...")

	return fs, nil
}

// PrepareFuncsFromFlagSet returns a list of zero or more known `document.PrepareDocumentFunc` functions
// based on the values in 'fs'.
func PrepareFuncsFromFlagSet(ctx context.Context, fs *flag.FlagSet) ([]document.PrepareDocumentFunc, error) {

	index_spelunker_v1, err := lookup.BoolVar(fs, FLAG_INDEX_SPELUNKER_V1)

	if err != nil {
		return nil, err
	}

	append_spelunker_v1, err := lookup.BoolVar(fs, FLAG_APPEND_SPELUNKER_V1)

	if err != nil {
		return nil, err
	}

	index_only_props, err := lookup.BoolVar(fs, FLAG_INDEX_PROPS)

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

	return prepare_funcs, nil
}

// BulkIndexerFromFlagSet returns a esutil.BulkIndexer instance derived from the values in 'fs'.
func BulkIndexerFromFlagSet(ctx context.Context, fs *flag.FlagSet) (esutil.BulkIndexer, error) {

	es_endpoint, err := lookup.StringVar(fs, FLAG_ES_ENDPOINT)

	if err != nil {
		return nil, err
	}

	es_index, err := lookup.StringVar(fs, FLAG_ES_INDEX)

	if err != nil {
		return nil, err
	}

	workers, err := lookup.IntVar(fs, FLAG_WORKERS)

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

	return bi, nil
}

// RunBulkIndexerOptionsFromFlagSet returns a `RunBulkIndexerOptions` instance derived from the values in 'fs'.
func RunBulkIndexerOptionsFromFlagSet(ctx context.Context, fs *flag.FlagSet) (*RunBulkIndexerOptions, error) {

	iterator_uri, err := lookup.StringVar(fs, FLAG_ITERATOR_URI)

	if err != nil {
		return nil, err
	}

	index_alt, err := lookup.BoolVar(fs, FLAG_INDEX_ALT)

	if err != nil {
		return nil, err
	}

	bi, err := BulkIndexerFromFlagSet(ctx, fs)

	if err != nil {
		return nil, err
	}

	prepare_funcs, err := PrepareFuncsFromFlagSet(ctx, fs)

	if err != nil {
		return nil, fmt.Errorf("Failed to derive default prepare funcs from flagset, %w", err)
	}

	iterator_paths := fs.Args()

	opts := &RunBulkIndexerOptions{
		BulkIndexer:   bi,
		PrepareFuncs:  prepare_funcs,
		IteratorURI:   iterator_uri,
		IteratorPaths: iterator_paths,
		IndexAltFiles: index_alt,
	}

	return opts, nil
}

// RunBulkIndexerWithFlagSet will "bulk" index a set of Who's On First documents with configuration details defined in 'fs'.
func RunBulkIndexerWithFlagSet(ctx context.Context, fs *flag.FlagSet) (*esutil.BulkIndexerStats, error) {

	opts, err := RunBulkIndexerOptionsFromFlagSet(ctx, fs)

	if err != nil {
		return nil, err
	}

	return RunBulkIndexer(ctx, opts)
}

// RunBulkIndexer will "bulk" index a set of Who's On First documents with configuration details defined in 'opts'.
func RunBulkIndexer(ctx context.Context, opts *RunBulkIndexerOptions) (*esutil.BulkIndexerStats, error) {

	bi := opts.BulkIndexer
	prepare_funcs := opts.PrepareFuncs
	iterator_uri := opts.IteratorURI
	iterator_paths := opts.IteratorPaths
	index_alt := opts.IndexAltFiles

	iter_cb := func(ctx context.Context, path string, fh io.ReadSeeker, args ...interface{}) error {

		body, err := io.ReadAll(fh)

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

	iter, err := iterator.NewIterator(ctx, iterator_uri, iter_cb)

	if err != nil {
		return nil, err
	}

	t1 := time.Now()

	err = iter.IterateURIs(ctx, iterator_paths...)

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
