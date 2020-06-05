package document

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tidwall/gjson"
)

func ExtractProperties(ctx context.Context, body []byte) ([]byte, error) {

	props_rsp := gjson.GetBytes(body, "properties")

	if !props_rsp.Exists() {
		msg := fmt.Sprintf("Missing propeties element.")
		return nil, errors.New(msg)
	}

	props_body, err := json.Marshal(props_rsp.Value())

	if err != nil {
		return nil, err
	}

	return props_body, nil

}
