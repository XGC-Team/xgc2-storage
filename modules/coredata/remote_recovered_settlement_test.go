package coredata_test

import (
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// Recovery joins an interrupted peer without reporting that its action
// succeeded. Existing open links must fit their original funded capacity.
func TestRecoveredRemoteSettlementUsesExistingReservation(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("owner")); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: "owner", RootRunID: "owner", TargetID: "agent", RunID: "child", CallNodeID: "call", BootID: "old-boot"}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	_, reservedBytes := usageRows(t, f)
	settlement := model.RemoteRunSettlement{Disposition: "recovered", FinishedAt: epoch}
	if changed, err := f.core.SettleRemoteRun(f.ctx, link, settlement); err != nil || !changed {
		t.Fatalf("settle recovered relation: changed=%v err=%v", changed, err)
	}
	_, settledBytes := usageRows(t, f)
	if settledBytes != reservedBytes {
		t.Fatalf("settlement changed prefunded capacity: %d -> %d", reservedBytes, settledBytes)
	}
	if changed, err := f.core.MarkRemoteRunReleased(f.ctx, link, epoch); err != nil || !changed {
		t.Fatalf("mark release: changed=%v err=%v", changed, err)
	}
	if _, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
		t.Fatalf("resolve recovered relation: %v", err)
	}
}
