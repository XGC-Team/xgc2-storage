package coredata

import (
	"strings"
	"testing"
)

func TestGetSessionsUsesIdentityIndex(t *testing.T) {
	db, ctx := fixture(t)
	rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+sessionBatchQuery(2), "scope", "one", "two")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan += detail
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plan, "SCAN ") || !strings.Contains(plan, "SEARCH ") || !strings.Contains(plan, "scope=? AND id=?") {
		t.Fatalf("batch lookup does not use session identity index: %s", plan)
	}
}
