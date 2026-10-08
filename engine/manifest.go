package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/XGC-Team/xgc2-storage/api"
)

func fail(code, message string) error { return &api.Error{Code: code, Message: message} }
func identifier(v string) bool {
	if v == "" || len(v) > 128 {
		return false
	}
	for _, c := range v {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("._-", c)) {
			return false
		}
	}
	return true
}
func key(v string) bool {
	return v != "" && len(v) <= 512 && utf8.ValidString(v) && !strings.ContainsAny(v, "\x00\r\n")
}
func revision(v string) (int64, error) {
	n, e := strconv.ParseInt(v, 10, 64)
	if e != nil || n < 0 || strconv.FormatInt(n, 10) != v {
		return 0, fail("invalid_argument", "canonical nonnegative decimal revision required")
	}
	return n, nil
}
func DecodeManifest(r io.Reader) (api.Manifest, error) {
	var m api.Manifest
	limited := &io.LimitedReader{R: r, N: (4 << 20) + 1}
	d := json.NewDecoder(limited)
	d.DisallowUnknownFields()
	if err := d.Decode(&m); err != nil {
		return m, err
	}
	var tail any
	if err := d.Decode(&tail); err != io.EOF {
		return m, fail("invalid_argument", "one manifest required")
	}
	if limited.N == 0 {
		return m, fail("resource_exhausted", "manifest exceeds 4 MiB")
	}
	return m, ValidateManifest(m)
}
func ValidateManifest(m api.Manifest) error {
	if m.Format != "storage-v1" || len(m.Namespaces) == 0 || len(m.Namespaces) > 64 {
		return fail("invalid_argument", "invalid manifest format/namespace count")
	}
	seen := map[string]bool{}
	for _, n := range m.Namespaces {
		if !identifier(n.ID) || !identifier(n.Owner) || !identifier(n.Schema) || seen[n.ID] || n.MaxScopes <= 0 || n.MaxScopes > 4096 || n.MaxReceipts <= 0 || n.MaxReceipts > 1000000 || n.ReceiptTTLSeconds <= 0 || n.ReceiptTTLSeconds > 31*86400 || len(n.Collections)+len(n.Modules) == 0 || len(n.Collections) > 256 || len(n.Modules) > 16 {
			return fail("invalid_argument", "namespace owner/schema/limits required")
		}
		seen[n.ID] = true
		mods := map[string]bool{}
		for _, m := range n.Modules {
			if !identifier(m.ID) || !identifier(m.Schema) || len(m.Digest) != 64 || mods[m.ID] || len(m.Operations) == 0 || len(m.Operations) > 32 {
				return fail("invalid_argument", "reviewed named module/schema/digest/operations required")
			}
			mods[m.ID] = true
			if _, e := hex.DecodeString(m.Digest); e != nil {
				return fail("invalid_argument", "module digest must be SHA256")
			}
			ops := map[string]bool{}
			for _, o := range m.Operations {
				if !identifier(o.ID) || ops[o.ID] || o.MaxRequestBytes < 1 || o.MaxRequestBytes > api.MaxNamedRequestBytes || o.MaxResponseBytes < 1 || o.MaxResponseBytes > api.MaxNamedResponseBytes {
					return fail("invalid_argument", "named operation bounds required")
				}
				ops[o.ID] = true
			}
		}
		cols := map[string]bool{}
		for _, c := range n.Collections {
			if !identifier(c.ID) || cols[c.ID] || c.MaxRecordBytes <= 0 || c.MaxRecordBytes > 3<<20 || c.MaxRecords <= 0 || c.MaxRecords > 10000000 || c.MaxBytes <= 0 || c.Retention == "" || c.Recovery == "" || len(c.Indexes) > 16 {
				return fail("invalid_argument", "collection identity/limits/retention/recovery required")
			}
			cols[c.ID] = true
			idx := map[string]bool{}
			for _, i := range c.Indexes {
				if !identifier(i.ID) || idx[i.ID] || len(i.Fields) == 0 || len(i.Fields) > 8 {
					return fail("invalid_argument", "invalid index")
				}
				idx[i.ID] = true
				for _, f := range i.Fields {
					if !identifier(f) {
						return fail("invalid_argument", "invalid index field")
					}
				}
			}
		}
	}
	return nil
}
func hash(v any) string {
	b, _ := json.Marshal(v)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
func scopeID(s api.Scope) string { b, _ := json.Marshal(s); return string(b) }
func canonicalObject(raw json.RawMessage) (json.RawMessage, map[string]any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v map[string]any
	if err := d.Decode(&v); err != nil || v == nil {
		return nil, nil, fail("invalid_argument", "record must be a JSON object")
	}
	var tail any
	if err := d.Decode(&tail); err != io.EOF {
		return nil, nil, fail("invalid_argument", "one JSON object required")
	}
	b, err := json.Marshal(v)
	return b, v, err
}
func indexTuple(data map[string]any, idx api.Index) (string, bool, error) {
	values := make([]any, 0, len(idx.Fields))
	nullable := false
	for _, f := range idx.Fields {
		v := data[f]
		if v == nil {
			nullable = true
		}
		switch v.(type) {
		case nil, string, bool, json.Number:
		default:
			return "", false, fail("invalid_argument", fmt.Sprintf("index %s requires scalar field %s", idx.ID, f))
		}
		values = append(values, v)
		if number, ok := v.(json.Number); ok {
			if len(number) > 128 {
				return "", false, fail("resource_exhausted", "index number too large")
			}
			if pos := strings.IndexAny(string(number), "eE"); pos >= 0 {
				exponent, err := strconv.ParseInt(string(number)[pos+1:], 10, 32)
				if err != nil || exponent < -128 || exponent > 128 {
					return "", false, fail("resource_exhausted", "indexed number exponent outside -128..128")
				}
			}
			rational, ok := new(big.Rat).SetString(string(number))
			if !ok {
				return "", false, fail("invalid_argument", "invalid indexed number")
			}
			values[len(values)-1] = map[string]string{"number": rational.RatString()}
		}
	}
	b, e := json.Marshal(values)
	if len(b) > 4096 {
		return "", false, fail("resource_exhausted", "index tuple too large")
	}
	return string(b), nullable, e
}
