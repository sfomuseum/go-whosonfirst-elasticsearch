package document

import (
	"context"
	"fmt"
	"github.com/sfomuseum/go-edtf"
	"github.com/sfomuseum/go-edtf/parser"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	_ "log"
)

type date_span struct {
	start int64
	end   int64
}

type date_range struct {
	outer *date_span
	inner *date_span
}

// AppendEDTFRanges appends numeric date ranges derived from `edtf:inception` and `edtf:cessation` properties
// to a Who's On First document.
func AppendEDTFRanges(ctx context.Context, body []byte) ([]byte, error) {

	props := gjson.ParseBytes(body)

	props_rsp := gjson.GetBytes(body, "properties")

	if props_rsp.Exists() {
		props = props_rsp
	}

	inception_range, err := deriveRanges(props, "edtf:inception")

	if err != nil {
		return nil, fmt.Errorf("Failed to derive inception ranges, %w", err)
	}

	cessation_range, err := deriveRanges(props, "edtf:cessation")

	if err != nil {
		return nil, fmt.Errorf("Failed to derive cessation ranges, %w", err)
	}

	to_assign := make(map[string]int64)

	if inception_range != nil {
		to_assign["date:inception_inner_start"] = inception_range.inner.start
		to_assign["date:inception_inner_end"] = inception_range.inner.end
		to_assign["date:inception_outer_start"] = inception_range.outer.start
		to_assign["date:inception_outer_end"] = inception_range.outer.end
	}

	if cessation_range != nil {
		to_assign["date:cessation_inner_start"] = cessation_range.inner.start
		to_assign["date:cessation_inner_end"] = cessation_range.inner.end
		to_assign["date:cessation_outer_start"] = cessation_range.outer.start
		to_assign["date:cessation_outer_end"] = cessation_range.outer.end
	}

	for k, v := range to_assign {

		path := k

		if props_rsp.Exists() {
			path = fmt.Sprintf("properties.%s", k)
		}

		body, err = sjson.SetBytes(body, path, v)

		if err != nil {
			return nil, fmt.Errorf("Failed to assign %s (%d), %w", path, v, err)
		}
	}

	return body, nil
}

func deriveRanges(props gjson.Result, path string) (*date_range, error) {

	edtf_rsp := props.Get(path)

	if !edtf_rsp.Exists() {
		return nil, nil
	}

	edtf_str := edtf_rsp.String()

	if !isValid(edtf_str) {
		return nil, nil
	}

	edtf_dt, err := parser.ParseString(edtf_str)

	if err != nil {
		return nil, fmt.Errorf("Failed to parse '%s', %w", path, err)
	}

	start := edtf_dt.Start
	end := edtf_dt.End

	start_lower := start.Lower
	start_upper := start.Upper

	end_lower := end.Lower
	end_upper := end.Upper

	if start_lower == nil {
		return nil, nil
	}

	if start_upper == nil {
		return nil, nil
	}

	if end_lower == nil {
		return nil, nil
	}

	if end_upper == nil {
		return nil, nil
	}

	start_lower_ts := start_lower.Timestamp
	start_upper_ts := start_upper.Timestamp

	end_lower_ts := end_lower.Timestamp
	end_upper_ts := end_upper.Timestamp

	if start_lower_ts == nil {
		return nil, nil
	}

	if start_upper_ts == nil {
		return nil, nil
	}

	if end_lower_ts == nil {
		return nil, nil
	}

	if end_upper_ts == nil {
		return nil, nil
	}

	outer_start := start_lower_ts.Unix()
	outer_end := end_upper_ts.Unix()

	inner_start := start_upper_ts.Unix()
	inner_end := end_lower_ts.Unix()

	outer := &date_span{
		start: outer_start,
		end:   outer_end,
	}

	inner := &date_span{
		start: inner_start,
		end:   inner_end,
	}

	r := &date_range{
		outer: outer,
		inner: inner,
	}

	return r, nil
}

func isValid(edtf_str string) bool {

	if isOpen(edtf_str) {
		return false
	}

	if isUnknown(edtf_str) {
		return false
	}

	if isUnspecified(edtf_str) {
		return false
	}

	return true
}

func isOpen(edtf_str string) bool {

	switch edtf_str {
	case edtf.OPEN, edtf.OPEN_2012:
		return true
	default:
		return false
	}
}

func isUnknown(edtf_str string) bool {

	switch edtf_str {
	case edtf.UNKNOWN, edtf.UNKNOWN_2012:
		return true
	default:
		return false
	}
}

func isUnspecified(edtf_str string) bool {

	switch edtf_str {
	case edtf.UNSPECIFIED, edtf.UNSPECIFIED_2012:
		return true
	default:
		return false
	}

}
