package index

import (
	"bytes"
	"context"
	"encoding/json"
	es "github.com/elastic/go-elasticsearch"
	esapi "github.com/elastic/go-elasticsearch/esapi"
	"io"
)

func IndexDocument(ctx context.Context, es_client *es.Client, es_index string, doc_id string, doc interface{}) error {

	b, err := json.Marshal(doc)

	if err != nil {
		return err
	}

	fh := bytes.NewReader(b)

	return IndexDocumentWithReader(ctx, es_client, es_index, doc_id, fh)
}

func IndexDocumentWithReader(ctx context.Context, es_client *es.Client, es_index string, doc_id string, fh io.Reader) error {

	select {
	case <-ctx.Done():
		return nil
	default:
		// pass
	}

	req := esapi.IndexRequest{
		Index:      es_index,
		DocumentID: doc_id,
		Body:       fh,
		Refresh:    "true",
	}

	_, err := req.Do(ctx, es_client)

	if err != nil {
		return err
	}

	return nil
}
