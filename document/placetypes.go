package document

import (
	"context"
	"errors"
	"fmt"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/whosonfirst/go-whosonfirst-placetypes"
)

func AppendPlacetypeDetails(ctx context.Context, body []byte) ([]byte, error) {

	var root gjson.Result

	props_rsp := gjson.GetBytes(body, "properties")

	if props_rsp.Exists() {
		root = props_rsp
	}

	pt_rsp := root.Get("wof:placetype")

	if !pt_rsp.Exists() {
		return nil, errors.New("Missing wof:placetype property")
	}

	str_pt := pt_rsp.String()

	if !placetypes.IsValidPlacetype(str_pt) {
		return body, nil
	}

	pt, err := placetypes.GetPlacetypeByName(str_pt)

	if err != nil {
		return nil, err
	}

	details := map[string]interface{}{
		"wof:placetype_id":    pt.Id,
		"wof:placetype_names": []string{str_pt},
	}

	for k, v := range details {

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
