package coredata

import (
	"bytes"
	"encoding/json"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// errorSummary validates the execution owner's preview without interpreting its
// business error shape. Short diagnostics remain byte-exact, including scalars.
func errorSummary(full, supplied []byte) ([]byte, error) {
	if len(full) <= model.MaxRunErrorSummaryBytes {
		if len(supplied) != 0 && !bytes.Equal(full, supplied) {
			return nil, failure("invalid_argument", "a short error summary must equal its full error")
		}
		return full, nil
	}
	if len(supplied) == 0 {
		return nil, failure("invalid_argument", "a large error requires its bounded observation summary")
	}
	return jsonField("error summary", supplied, model.MaxRunErrorSummaryBytes, true, false)
}

func cleanupCardinality(raw []byte) (int, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0, nil
	}
	var entries []json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		return 0, failure("invalid_argument", "cleanup errors must be an array or null")
	}
	return len(entries), nil
}
