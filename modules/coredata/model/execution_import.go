package model

// ExecutionImport preserves authored receipts and the exact old event frontier.
// It is an offline input, never a normal execution.commit or Named operation.
type ExecutionImport struct {
	Commands           []CommandReceipt  `json:"commands"`
	Events             []ExecutionEvent  `json:"events"`
	EventFrontier      string            `json:"event_frontier"`
	LegacyStreamID     string            `json:"legacy_stream_id"`
	SourceRowFrontiers map[string]string `json:"source_row_frontiers"`
}
