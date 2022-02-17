package document

import (
	"context"
	"fmt"
	"github.com/tidwall/gjson"
	"testing"
)

func TestAppendEDTFRanges(t *testing.T) {

	ctx := context.Background()

	docs := []string{
		`{"properties": {"edtf:inception": "1970-01-01", "edtf:cessation": "1980-07-01" }}`,
		`{"properties": {"edtf:inception": "~1969", "edtf:cessation": "198X" }}`,
		`{"properties": {"edtf:inception": "~1937-01", "edtf:cessation": "2020-~04" }}`,
		`{"properties": {"edtf:inception": "2021-10-10T00:24:00Z", "edtf:cessation": "2021-10-10T00:24:00Z" }}`,
		`{"properties": {"edtf:inception": "2021-10-10T00:24:00Z", "edtf:cessation": "2021-10-10T00:24:00Z" }}`,
	}

	for _, body := range docs {

		new_body, err := AppendEDTFRanges(ctx, []byte(body))

		if err != nil {
			t.Fatalf("Failed to append EDTF ranges, %v", err)
		}

		expected := []string{
			"date:inception_inner_start",
			"date:inception_inner_end",
			"date:inception_outer_start",
			"date:inception_outer_end",
			"date:cessation_inner_start",
			"date:cessation_inner_end",
			"date:cessation_outer_start",
			"date:cessation_outer_end",
		}

		for _, k := range expected {

			path := fmt.Sprintf("properties.%s", k)

			rsp := gjson.GetBytes(new_body, path)

			if !rsp.Exists() {
				t.Fatalf("Updated body missing %s property (%s)", path, string(new_body))
			}
		}

		// fmt.Println(string(new_body))
	}
}
