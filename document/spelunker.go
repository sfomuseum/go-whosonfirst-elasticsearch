package document

import (
	"context"
)

func PrepareSpeunkerV1Document(ctx context.Context, body []byte) ([]byte, error) {

	var prepped []byte
	var err error

	prepped, err = ExtractProperties(ctx, body)

	if err != nil {
		return nil, err
	}

	prepped, err = AppendNameCounts(ctx, prepped)

	if err != nil {
		return nil, err
	}

	return prepped, nil
}
