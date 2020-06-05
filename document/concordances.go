package document

import (
	"context"
	"errors"
	"fmt"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func AppendConcordancesStats(ctx context.Context, body []byte) ([]byte, error) {

	var root gjson.Result

	props_rsp := gjson.GetBytes(body, "properties")

	if props_rsp.Exists() {
		root = props_rsp
	}

	concordances_rsp := root.Get("wof:concordances")

	if !concordances_rsp.Exists() {
		return nil, errors.New("Missing wof:concordances property")
	}

	sources := make([]string, 0)

	for k, _ := range concordances_rsp.Map() {
		sources = append(sources, k)
	}

	stats := map[string]interface{}{
		"wof:concordances_sources":  sources,
		"counts:concordances_total": len(sources),
	}

	var err error

	for k, v := range stats {

		path := k

		if props_rsp.Exists() {
			path = fmt.Sprintf("properties.%s", k)
		}

		body, err = sjson.SetBytes(body, path, v)

		if err != nil {
			return nil, err
		}
	}

	return body, nil
}
