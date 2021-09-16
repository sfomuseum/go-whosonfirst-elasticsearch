// package document provides methods for updating a single Who's On First document for indexing in Elasticsearch.
package document

import (
	"context"
)

// type PrepareDocumentFunc is a common method signature updating a Who's On First document for indexing in Elasticsearch.
type PrepareDocumentFunc func(context.Context, []byte) ([]byte, error)
