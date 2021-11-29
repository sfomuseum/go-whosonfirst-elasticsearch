package index

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

	// retry := backoff.NewExponentialBackOff()

	es_client, err := es.NewClient(es.SetURL(es_endpoint))

	if err != nil {
		return nil, err
	}

	/*
		_, err = es_client.Indices.Create(es_index)

		if err != nil {
			return nil, err
		}


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
	*/

	bi := es.NewBulkProcessorService(es_client)

	bi.FlushInterval(30 * time.Second)
	bi.Workers(workers)

	bp, err := bi.Do()

	if err != nil {
		return nil, fmt.Errorf("Failed to 'Do' indexer, %w", err)
	}

	if err != nil {
		return nil, fmt.Errorf("Failed to start indexer, %w", err)
	}

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

		pt_rsp := gjson.GetBytes(body, "properties.wof:placetype")

		if !pt_rsp.Exists() {
			msg := fmt.Sprintf("%s is missing properties.wof:placetype", path)
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

		/*
			enc_f, err := json.Marshal(f)

			if err != nil {
				msg := fmt.Sprintf("Failed to marshal %s, %v", path, err)
				return errors.New(msg)
			}
		*/

		// log.Println(string(enc_f))

		bulk_item := &es.BulkIndexRequest{}
		bulk_item.Id(doc_id)
		bulk_item.Index(es_index)
		bulk_item.Type(pt_rsp.String())

		bulk_item.Doc(f)

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
