package main

import (
	_ "github.com/whosonfirst/go-whosonfirst-index/fs"
)

import (
	"context"
	"errors"
	"flag"
	"fmt"
	es "github.com/elastic/go-elasticsearch/v7"
	esapi "github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/whosonfirst/go-whosonfirst-index"
	"github.com/tidwall/gjson"
	"io"
	"io/ioutil"
	"log"
	"strconv"
)

func main() {

	es_endpoint := flag.String("elasticsearch-endpoint", "http://localhost:9200", "...")
	es_index := flag.String("elasticsearch-index", "millsfield", "...")

	idx_uri := flag.String("indexer-uri", "repo://", "...")

	// bulk := flag.Bool("bulk", false, "...")

	flag.Parse()

	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	es_cfg := es.Config{
		Addresses: []string{*es_endpoint},
	}

	es_client, err := es.NewClient(es_cfg)

	if err != nil {
		log.Fatalf("Failed to create ES client, %v", err)
	}

	_, err = es_client.Indices.Create(*es_index)

	if err != nil {
		log.Fatalf("Failed to create ES index, %v", err)
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

		if !id_rsp.Exists(){
		msg := fmt.Sprintf("%s is missing properties.wof:id", path)
		   return errors.New(msg)
		}

		wof_id := id_rsp.Int()
		doc_id := strconv.FormatInt(wof_id, 10)

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

		switch rsp.StatusCode {
		case 200, 201:
		// pass
		default:
			body, _ := ioutil.ReadAll(rsp.Body)
			// return errors.New(string(body))

			log.Println(string(body))
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
