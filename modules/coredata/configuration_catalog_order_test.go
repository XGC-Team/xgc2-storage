package coredata

import "testing"

func TestCatalogNameCollationComparesDecimalRuns(t *testing.T) {
	for _, test := range []struct{ first, second string }{
		{"fs150-2", "fs150-10"},
		{"fleet-01-unit-2", "fleet-1-unit-10"},
		{"robot-999999999999999999999", "robot-1000000000000000000000"},
		{"robot-0", "robot-1"},
		{"robot-2", "scout-1"},
	} {
		if compareCatalogNames(test.first, test.second) >= 0 || compareCatalogNames(test.second, test.first) <= 0 {
			t.Errorf("natural comparison of %q and %q is inconsistent", test.first, test.second)
		}
	}
	if compareCatalogNames("robot-01", "robot-1") != 0 {
		t.Fatal("equivalent numbers must use the query's deterministic secondary keys")
	}
}
