package coredata

import (
	"strings"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"modernc.org/sqlite"
)

func init() {
	if err := sqlite.RegisterCollationUtf8("xgc_catalog_natural", compareCatalogNames); err != nil {
		panic(err)
	}
}

func catalogPageOrdering(order model.ConfigurationCatalogOrder) string {
	const names = "r.name_key COLLATE xgc_catalog_natural, r.name_key, r.id"
	// Core writes UTC RFC3339Nano. Removing Z makes whole seconds sort
	// before fractional values within the same second.
	const updated = "rtrim(json_extract(CAST(r.body AS TEXT),'$.updated_at'),'Z')"
	switch order {
	case model.ConfigurationCatalogNameOrder:
		return names
	case model.ConfigurationCatalogOldestOrder:
		return updated + " ASC, " + names
	case model.ConfigurationCatalogNewestOrder:
		return updated + " DESC, " + names
	default:
		return ""
	}
}

// Decimal runs compare numerically without a fixed integer range. SQL breaks
// equal values by the original normalized name key and resource identity.
func compareCatalogNames(a, b string) int {
	for len(a) > 0 && len(b) > 0 {
		if catalogDigit(a[0]) && catalogDigit(b[0]) {
			x, y := catalogDigitRun(a), catalogDigitRun(b)
			an, bn := strings.TrimLeft(a[:x], "0"), strings.TrimLeft(b[:y], "0")
			if len(an) != len(bn) {
				return len(an) - len(bn)
			}
			if cmp := strings.Compare(an, bn); cmp != 0 {
				return cmp
			}
			a, b = a[x:], b[y:]
			continue
		}
		if a[0] != b[0] {
			return int(a[0]) - int(b[0])
		}
		a, b = a[1:], b[1:]
	}
	return strings.Compare(a, b)
}

func catalogDigit(c byte) bool { return '0' <= c && c <= '9' }

func catalogDigitRun(s string) int {
	i := 0
	for i < len(s) && catalogDigit(s[i]) {
		i++
	}
	return i
}
