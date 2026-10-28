package coredata

import (
	"encoding/json"
	"errors"
)

// The scalar is supplied only by the normalized domain summary projector. It
// remains covered by the immutable catalog digest and is never caller SQL.
func configurationSummarySelector(summary []byte) (string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(summary, &fields); err != nil || fields == nil {
		return "", errors.New("invalid summary")
	}
	raw, present := fields["_catalogSelector"]
	if !present {
		return "", nil
	}
	var selector string
	if string(raw) == "null" || json.Unmarshal(raw, &selector) != nil || selector != "" && !textKey(selector) {
		return "", errors.New("invalid catalog selector")
	}
	return selector, nil
}
