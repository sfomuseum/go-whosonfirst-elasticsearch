package document

import (
	"context"
)

func PrepareSpelunkerV1Document(ctx context.Context, body []byte) ([]byte, error) {

	prepped, err := ExtractProperties(ctx, body)

	if err != nil {
		return nil, err
	}

	return AppendSpelunkerV1Properties(ctx, prepped)
}

func AppendSpelunkerV1Properties(ctx context.Context, body []byte) ([]byte, error) {

	var err error

	body, err = AppendNameStats(ctx, body)

	if err != nil {
		return nil, err
	}

	body, err = AppendConcordancesStats(ctx, body)

	if err != nil {
		return nil, err
	}

	body, err = AppendPlacetypeDetails(ctx, body)

	if err != nil {
		return nil, err
	}

	// to do: categories and machine tags...

	return body, nil
}
