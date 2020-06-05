package document

import (
	"context"
)

type PrepareDocumentFunc func(context.Context, []byte) ([]byte, error)
