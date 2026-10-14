package coredata

import (
	"bytes"
	"compress/gzip"
	"io"
)

// Run JSON is losslessly compressed in schema 3. Public values and their
// decoded size limits stay unchanged; the live quota counts stored bytes.
func encodeRunPayload(raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return []byte{}, nil
	}
	var output bytes.Buffer
	writer, err := gzip.NewWriterLevel(&output, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err = writer.Write(raw); err != nil {
		return nil, err
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func decodeRunPayload(stored []byte, limit int) ([]byte, error) {
	if len(stored) == 0 {
		return nil, nil
	}
	input := bytes.NewReader(stored)
	reader, err := gzip.NewReader(input)
	if err != nil {
		return nil, failure("data_loss", "invalid compressed Run payload")
	}
	defer reader.Close()
	reader.Multistream(false)
	raw, err := io.ReadAll(io.LimitReader(reader, int64(limit)+1))
	if err != nil || len(raw) > limit || input.Len() != 0 {
		return nil, failure("data_loss", "compressed Run payload is corrupt or exceeds its decoded byte limit")
	}
	return raw, nil
}
