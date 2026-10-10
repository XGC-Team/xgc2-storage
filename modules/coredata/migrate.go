package coredata

import (
	"context"
	"database/sql"

	"github.com/XGC-Team/xgc2-storage/engine"
)

// legacyTables are the tables of the first Core data module, which held
// the configuration model and the workflow engine's relational facts.
var legacyTables = []string{
	"core_data_usage", "core_namespaces", "core_resources", "core_branches", "core_snapshots", "core_references", "core_changes",
	"core_configuration_domains", "core_configuration_receipts", "core_catalog_receipts",
	"core_groups", "core_group_members", "core_commands", "core_execution_events", "core_event_offsets", "core_event_sequences", "core_execution_identity",
}

// retiredCollections are the document collections of the workflow engine and of
// the Core packages that left with it: the Run ledger and its relations,
// Sessions, triggers, schedules, robot operations, process instances, Jobs,
// interactions, frozen run configurations and bundles, container fleet
// environments, MCP connections, the adapter runtime ledger and the App Store.
var retiredCollections = []string{
	"activation_tokens", "attempts", "child_group_members", "child_groups", "child_links", "definition_heads", "definitions", "effects", "execution_leases",
	"interaction_mutations", "interactions", "invocation_tasks", "invocations", "job_artifacts", "job_attempts", "job_capacity", "job_runs", "node_inputs", "node_outputs",
	"process_instances", "resource_bindings", "resource_bundles", "resource_gates", "resource_leases", "resource_members", "robot_attempts", "robot_batch_items",
	"robot_batches", "robot_bindings", "robot_connections", "robot_operations", "run_admission", "run_sequences", "run_tasks", "runs", "runtime_bindings", "runtime_groups",
	"runtime_observation_latest", "runtime_observations", "schedules", "session_bindings", "session_frontiers", "session_members", "session_stop_intents", "sessions",
	"trigger_activations", "trigger_completions", "trigger_events", "trigger_lifecycles", "trigger_listeners", "trigger_remote_activations", "trigger_remote_deliveries",
	"trigger_transitions", "waits", "workflow_jobs",
	"run-configurations", "workflow-bundles", "fleet-environments", "fleet-allocations", "fleet-generations", "mcp_connections", "mcp_invocations",
	"adapter-runtime-instances", "adapter-runtime-epochs", "adapter-runtime-processes", "adapter-runtime-sessions", "adapter-runtime-leases", "adapter-runtime-invocations",
	"adapter-runtime-keys", "appstore-apps", "appstore-details", "appstore-installs", "appstore-settings", "appstore-tags",
}

// retiredNamespaces held Core state that no longer exists as a whole.
var retiredNamespaces = []string{"core-panel-state"}

// legacy recognizes a first-version Core data installation by its complete table
// set. Anything in between is not a layout this release can safely migrate.
func legacy(ctx context.Context, q engine.Queryer) (int, error) {
	present := 0
	for _, table := range legacyTables {
		var n int
		if err := q.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&n); err != nil {
			return 0, err
		}
		present += n
	}
	switch present {
	case 0:
		return 0, nil
	case len(legacyTables):
		return 1, nil
	}
	return 0, failure("failed_precondition", "the database holds an unrecognized Core data layout; restore a backup or migrate with the release that wrote it")
}

// migrate1 upgrades the module from version 1 to 2. The configuration data
// (namespaces, resources, branches, commits, references, changes, domains and
// product receipts) stays exactly as it is. The workflow engine's data goes:
// its document collections, the command ledger, the event log and the sealed
// groups. The Run, Session and recording tables are created empty.
func migrate1(ctx context.Context, tx *sql.Tx, namespaces []string) error {
	for _, namespace := range namespaces {
		if err := engine.Retire(ctx, tx, namespace, retiredCollections...); err != nil {
			return err
		}
	}
	for _, namespace := range retiredNamespaces {
		if err := engine.Retire(ctx, tx, namespace); err != nil {
			return err
		}
	}
	for _, statement := range []string{
		"DROP VIEW core_group_events", "DROP VIEW core_child_links", "DROP TABLE core_group_members", "DROP TABLE core_groups",
		"DROP TABLE core_commands", "DROP TABLE core_execution_events", "DROP TABLE core_event_offsets", "DROP TABLE core_event_sequences",
		"DROP TABLE core_execution_identity",
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, RecordsSQL); err != nil {
		return err
	}
	// Version 1 also charged events and commands to this counter and never gave
	// anything back. The live total is what the remaining rows occupy.
	_, err := tx.ExecContext(ctx, `DELETE FROM core_data_usage;
INSERT INTO core_data_usage(scope,rows,bytes) SELECT scope,sum(rows),sum(bytes) FROM (
 SELECT scope,count(*) AS rows,coalesce(sum(length(body)),0) AS bytes FROM core_namespaces GROUP BY scope
 UNION ALL SELECT scope,count(*),coalesce(sum(length(body)),0) FROM core_resources GROUP BY scope
 UNION ALL SELECT scope,count(*),coalesce(sum(length(body)),0) FROM core_branches GROUP BY scope
 UNION ALL SELECT scope,count(*),coalesce(sum(length(body)+length(payload)+length(manifest)),0) FROM core_snapshots GROUP BY scope
 UNION ALL SELECT scope,count(*),coalesce(sum(length(body)),0) FROM core_references GROUP BY scope
 UNION ALL SELECT scope,count(*),coalesce(sum(length(body)),0) FROM core_changes GROUP BY scope
 UNION ALL SELECT scope,count(*),coalesce(sum(length(body)),0) FROM core_configuration_receipts GROUP BY scope
 UNION ALL SELECT scope,count(*),coalesce(sum(length(body)),0) FROM core_catalog_receipts GROUP BY scope
) GROUP BY scope`)
	return err
}
