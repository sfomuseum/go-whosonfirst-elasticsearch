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

	prepped, err = AppendNameStats(ctx, prepped)

	if err != nil {
		return nil, err
	}

	prepped, err = AppendConcordancesStats(ctx, prepped)

	if err != nil {
		return nil, err
	}

	prepped, err = AppendPlacetypeDetails(ctx, prepped)

	if err != nil {
		return nil, err
	}

	return prepped, nil
}
