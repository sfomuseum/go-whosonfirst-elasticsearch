package edtf

import (
	"fmt"
	"github.com/sfomuseum/go-edtf"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"strings"
)

func UpdateBytes(body []byte) (bool, []byte, error) {

	props := gjson.GetBytes(body, "properties")

	if !props.Exists() {
		return false, nil, fmt.Errorf("Missing properties")
	}

	changed := false

	for k, v := range props.Map() {

		if !strings.HasPrefix(k, "edtf:") {
			continue
		}

		path := fmt.Sprintf("properties.%s", k)

		var err error

		switch v.String() {
		case "open":

			body, err = sjson.SetBytes(body, path, edtf.OPEN)

			if err != nil {
				return false, nil, fmt.Errorf("Failed to assign %s, %w", path, err)
			}

			changed = true

		case "uuuu":

			body, err = sjson.SetBytes(body, path, edtf.UNKNOWN)

			if err != nil {
				return false, nil, fmt.Errorf("Failed to assign %s, %w", path, err)
			}

			changed = true

		default:
			// https://github.com/whosonfirst/go-whosonfirst-edtf/issues
			// pass
		}
	}

	return changed, body, nil
}
