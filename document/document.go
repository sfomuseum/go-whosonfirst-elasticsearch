package document

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tidwall/gjson"
)

type PrepareDocumentFunc func(context.Context, []byte) ([]byte, error)

func OnlyProps(ctx context.Context, body []byte) ([]byte, error) {

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
