package model

// RunExecutionOutcome marks a single-Run selection containing its full result
// and error plus bounded observation metadata, without unrelated payloads.
// It is returned by GetRunExecutionOutcome, not accepted as a list projection.
const RunExecutionOutcome RunProjection = "execution-outcome"
