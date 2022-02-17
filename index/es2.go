package index

// https://github.com/olivere/elastic/wiki/BulkProcessor
// https://gist.github.com/olivere/a1dd52fc28cdfbbd6d4f

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	// "github.com/cenkalti/backoff/v4"
	"github.com/sfomuseum/go-flags/lookup"
	"github.com/sfomuseum/go-whosonfirst-elasticsearch/document"
	"github.com/tidwall/gjson"
	"github.com/whosonfirst/go-whosonfirst-edtf"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/iterator"
	"github.com/whosonfirst/go-whosonfirst-uri"
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

	/*
		index_alt, err := lookup.BoolVar(fs, FLAG_INDEX_ALT)

		if err != nil {
			return nil, err
		}
	*/

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

	es_client, err := es.NewClient(es.SetURL(es_endpoint))

	if err != nil {
		return nil, err
	}

	beforeCallback := func(executionId int64, req []es.BulkableRequest) {
		// log.Printf("Before commit %d, %d items\n", executionId, len(req))
	}

	afterCallback := func(executionId int64, requests []es.BulkableRequest, rsp *es.BulkResponse, err error) {

		if err != nil {
			log.Printf("Commit ID %d failed with error %v\n", executionId, err)
		}
	}

	// ugh, method chaining...

	bp, err := es_client.BulkProcessor().
		Name("Indexer").
		FlushInterval(30 * time.Second).
		Workers(workers).
		BulkActions(1000).
		Stats(true).
		Before(beforeCallback).
		After(afterCallback).
		Do()

	if err != nil {
		return nil, fmt.Errorf("Failed to 'Do' indexer, %w", err)
	}

	iter_cb := func(ctx context.Context, path string, fh io.ReadSeeker, args ...interface{}) error {

		_, uri_args, err := uri.ParseURI(path)

		// Insert debate about whether or not to be strict in situations like this here.
		// The problem with being strict is that it causes things like the automated
		// Spelunker indexing to fail when, more likely, what we want is some sort of
		// abstract "notify someone" system to record events like this. For now we will
		// simply notify the log files.

		if err != nil {
			log.Printf("Failed to parse %s, %v", path, err)
			return nil
		}

		if uri_args.IsAlternate {
			return nil
		}

		body, err := io.ReadAll(fh)

		if err != nil {
			return err
		}

		id_rsp := gjson.GetBytes(body, "properties.wof:id")

		if !id_rsp.Exists() {
			msg := fmt.Sprintf("%s is missing properties.wof:id", path)
			return errors.New(msg)
		}

		pt_rsp := gjson.GetBytes(body, "properties.wof:placetype")

		if !pt_rsp.Exists() {
			log.Printf("%s is missing properties.wof:placetype\n", path)
			return nil
			// return errors.New(msg)
		}

		wof_id := id_rsp.Int()
		doc_id := strconv.FormatInt(wof_id, 10)

		// TO DO: move this in to the relevant document.Prepare method
		// https://github.com/whosonfirst/go-whosonfirst-edtf

		_, body, err = edtf.UpdateBytes(body)

		if err != nil {
			log.Printf("Failed to apply EDTF updates for %s, %v\n", path, err)
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
				log.Printf("Failed to prepare body for %s (%v), %v\n", path, f, err)
				return nil
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

		// Ugh... method chaning

		bulk_item := es.NewBulkIndexRequest().
			Id(doc_id).
			Index(es_index).
			Type(pt_rsp.String()).
			Doc(f)

		bp.Add(bulk_item)
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

	err = bp.Flush()

	if err != nil {
		return nil, fmt.Errorf("Failed to flush indexer, %w", err)
	}

	err = bp.Close()

	if err != nil {
		return nil, fmt.Errorf("Failed to close indexer, %w", err)
	}

	log.Printf("Processed %d files in %v\n", iter.Seen, time.Since(t1))

	stats := bp.Stats()
	return &stats, nil
}
