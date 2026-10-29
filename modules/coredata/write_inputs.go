package coredata

import (
	"bytes"
	"context"
	"slices"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func canceledWrite(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return engine.WriteRefused(err)
	}
	return nil
}

func ownedPointer[T any](p *T) *T {
	if p == nil {
		return nil
	}
	value := *p
	return &value
}

func ownedCatalogRead(q model.ConfigurationCatalogRead) model.ConfigurationCatalogRead {
	q.ParentID = ownedPointer(q.ParentID)
	return q
}

// A canceled caller can return while the same transaction is still inside a
// driver call. These copies give that admitted attempt ownership of its input
// until rollback/settlement, without re-encoding a normal plan.
func ownedRun(q model.NewRun) model.NewRun {
	q.Inputs = cloneBounded(q.Inputs, model.MaxRunInputsBytes)
	q.Trigger = cloneBounded(q.Trigger, model.MaxRunTriggerBytes)
	q.SourceObservation = cloneBounded(q.SourceObservation, model.MaxRunSourceObservationBytes)
	return q
}

func ownedFinish(q model.RunFinish) model.RunFinish {
	q.Error = cloneBounded(q.Error, model.MaxRunErrorBytes)
	q.ErrorSummary = cloneBounded(q.ErrorSummary, model.MaxRunErrorSummaryBytes)
	q.Result = cloneBounded(q.Result, model.MaxRunResultBytes)
	q.Nodes = cloneBounded(q.Nodes, model.MaxRunNodesBytes)
	q.CleanupErrors = cloneBounded(q.CleanupErrors, model.MaxRunCleanupErrorsBytes)
	return q
}

// The typed validator checks length before reading an oversized field. Avoid
// copying invalid unbounded input solely for an attempt that cannot use it.
func cloneBounded(raw []byte, max int) []byte {
	if len(raw) > max {
		return raw
	}
	return bytes.Clone(raw)
}

func ownedSnapshot(q model.PreparedConfigurationSnapshot) model.PreparedConfigurationSnapshot {
	if !configurationSnapshotWithinRawBounds(q) {
		return q // existing owner validation rejects lengths before inspecting these bytes
	}
	q.Payload = bytes.Clone(q.Payload)
	q.Manifest = bytes.Clone(q.Manifest)
	q.Identity = bytes.Clone(q.Identity)
	q.CatalogSummary = bytes.Clone(q.CatalogSummary)
	q.Associations = slices.Clone(q.Associations)
	q.References = slices.Clone(q.References)
	q.Change.Nodes = slices.Clone(q.Change.Nodes)
	return q
}

func ownedSource(q *model.ConfigurationSourcePin) *model.ConfigurationSourcePin {
	if q == nil {
		return nil
	}
	pin := *q
	if q.Branch != nil {
		branch := *q.Branch
		pin.Branch = &branch
	}
	return &pin
}

func ownedResourceCreate(q model.ConfigurationResourceCreate) model.ConfigurationResourceCreate {
	q.Source = ownedSource(q.Source)
	q.Snapshot = ownedSnapshot(q.Snapshot)
	return q
}

func ownedResourceCommit(q model.ConfigurationResourceCommit) model.ConfigurationResourceCommit {
	q.Source = ownedSource(q.Source)
	if q.MoveTo != nil {
		move := *q.MoveTo
		q.MoveTo = &move
	}
	q.Snapshot = ownedSnapshot(q.Snapshot)
	return q
}

func ownedNamespaceWrite(q model.ConfigurationNamespaceWrite) model.ConfigurationNamespaceWrite {
	if q.Parent != nil {
		parent := *q.Parent
		q.Parent = &parent
	}
	return q
}

func ownedNamespaceClone(q model.ConfigurationNamespaceClone) model.ConfigurationNamespaceClone {
	if len(q.Namespaces) > MaxCloneNamespaces || len(q.Resources) > MaxCloneResources {
		return q // the existing transaction rejects these counts before reading entries
	}
	q.Namespaces = slices.Clone(q.Namespaces)
	q.Resources = slices.Clone(q.Resources)
	associations := 0
	for i := range q.Resources {
		associations += len(q.Resources[i].Snapshot.Associations)
		if associations > model.MaxConfigurationCloneAssociations {
			return q // the existing transaction rejects this before reading association bodies
		}
	}
	for i := range q.Resources {
		q.Resources[i] = ownedResourceCreate(q.Resources[i])
	}
	return q
}
