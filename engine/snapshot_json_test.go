package engine

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"strings"
	"testing"
)

func TestLargeEqualitySyntaxMatchesStandardJSON(t *testing.T) {
	seeds := [][]byte{
		[]byte(`null`), []byte(`true`), []byte(`false`), []byte(`-0.01E+2`),
		[]byte(`"escaped \" \\ \/ \b \f \n \r \t \u0061 \ud800"`),
		[]byte(` [ { "a" : [1, null, {"b":true}], "c": false }, [] ] `),
		[]byte(`"` + strings.Repeat("a", 25000) + `"`),
		[]byte(`[` + strings.Repeat("null,", 6000) + `false]`),
		[]byte(strings.Repeat("[", 10000) + `0` + strings.Repeat("]", 10000)),
		[]byte(strings.Repeat("[", 10001) + `0` + strings.Repeat("]", 10001)),
		{'"', 0xff, '"'},
	}
	check := func(raw []byte) {
		t.Helper()
		valid := json.Valid(raw)
		err := validateLargeEqualityJSON(context.Background(), raw)
		if (err == nil) != valid {
			t.Fatalf("syntax differs from encoding/json for %q: valid=%v err=%v", raw, valid, err)
		}
	}
	for _, raw := range seeds {
		check(raw)
	}
	// Check altered delimiters, escapes, controls, numbers and trailing bytes
	// against the standard parser rather than a second implementation oracle.
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < 1500; i++ {
		seed := seeds[rng.Intn(6)]
		raw := append([]byte(nil), seed...)
		for j := 0; j < 1+rng.Intn(3); j++ {
			switch rng.Intn(3) {
			case 0:
				raw = append(raw, byte(rng.Intn(128)))
			case 1:
				if len(raw) > 0 {
					raw[rng.Intn(len(raw))] = byte(rng.Intn(128))
				}
			case 2:
				if len(raw) > 0 {
					raw = raw[:rng.Intn(len(raw))]
				}
			}
		}
		check(raw)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := validateLargeEqualityJSON(ctx, seeds[6]); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled syntax walk: %v", err)
	}
}
