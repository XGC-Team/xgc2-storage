package engine_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func remoteSettlementFixture(t *testing.T) (*engine.Store, *coredata.Store, engine.Config, api.Scope, model.RemoteRunLink) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	scope := api.Scope{Namespace: "core", User: "operator", Workspace: "station"}
	manifest := api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{{ID: "core", Owner: "core", Schema: coredata.Schema, MaxScopes: 8, MaxReceipts: 100, ReceiptTTLSeconds: 3600, Modules: []string{model.Module}}}}
	cfg := engine.Config{Path: filepath.Join(dir, "storage.db"), Create: true, Manifest: manifest, Modules: []engine.Module{coredata.Module()}}
	db, err := engine.Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	store, err := coredata.New(db, scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.CreateRun(t.Context(), model.NewRun{ID: "root", TargetID: "core", WorkflowResourceID: "wf", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: "root", RootRunID: "root", TargetID: "agent", RunID: "remote", CallNodeID: "call", BootID: "boot", Phase: model.RemoteRunOpen}
	if err = store.ReserveRemoteRun(t.Context(), link); err != nil {
		t.Fatal(err)
	}
	if _, err = store.FinishRun(t.Context(), model.RunFinish{ID: "root", Status: model.RunCanceled, At: time.Now().UTC()}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	// These tests exercise the final durable DELETE, so the fixture must
	// first supply the real terminal and incoming-hold release evidence.
	if _, err = store.SettleRemoteRun(t.Context(), link, model.RemoteRunSettlement{Disposition: "canceled", FinishedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.MarkRemoteRunReleased(t.Context(), link, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	return db, store, cfg, scope, link
}

func remoteUnknown(t *testing.T, err error, attempted, settled bool) {
	t.Helper()
	var disposition *engine.WriteError
	var resolution *coredata.RemoteRunResolveError
	if !errors.As(err, &disposition) || disposition.Disposition != engine.WriteUnknown || disposition.Settled != settled || !errors.As(err, &resolution) || resolution.ValidatedAttempt != attempted {
		t.Fatalf("unknown resolution disposition=%+v resolution=%+v error=%v", disposition, resolution, err)
	}
	if !attempted && resolution.AttemptedLink != (model.RemoteRunLink{}) {
		t.Fatalf("unattempted resolution fabricated link evidence: %+v", resolution.AttemptedLink)
	}
}

func TestRemoteResolutionUnknownAfterRealCommitRetainsAttempt(t *testing.T) {
	db, store, cfg, scope, link := remoteSettlementFixture(t)
	injected := engine.InjectCommitAfterForModuleTest(t, db)
	// A wrong boot is a definite refusal and cannot consume or fabricate the
	// later exact delete attempt.
	wrong := link
	wrong.BootID = "wrong-boot"
	if _, err := store.ResolveRemoteRun(t.Context(), wrong, model.RunRetentionPolicy{}); !engine.DefinitelyNotCommitted(err) {
		t.Fatalf("wrong boot did not roll back: %v", err)
	}
	before, _ := db.Stats()
	result, err := store.ResolveRemoteRun(t.Context(), link, model.RunRetentionPolicy{})
	remoteUnknown(t, err, true, true)
	var resolution *coredata.RemoteRunResolveError
	if !errors.As(err, &resolution) || resolution.AttemptedLink != link {
		t.Fatalf("validated attempt lost its exact identity: %+v %v", resolution, err)
	}
	if !errors.Is(err, injected) || result != (model.RemoteRunResolveResult{}) {
		t.Fatalf("unacknowledged commit returned a result or lost its cause: %+v %v", result, err)
	}
	fact, err := resolution.WaitRevalidation(t.Context())
	if err != nil || !fact.Released || !fact.LastForOwner || !fact.LastForRoot || fact.Link != link {
		t.Fatalf("exact fault revalidation: %+v %v", fact, err)
	}
	after, _ := db.Stats()
	if after.Calls-before.Calls != 1 || after.WritersQueued != 0 || after.ReadersActive != 0 {
		t.Fatalf("unknown write did not join: before=%+v after=%+v", before, after)
	}
	if _, err := store.ReadRemoteRunLink(t.Context(), link); err == nil {
		t.Fatal("real commit did not delete the exact relation")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	reopened, err := engine.Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	store, err = coredata.New(reopened, scope)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := store.ResolveRemoteRun(t.Context(), link, model.RunRetentionPolicy{})
	if err != nil || replay.Changed || replay.LastForRoot {
		t.Fatalf("replay invented the lost release edge: %+v %v", replay, err)
	}
	t.Logf("real deletion survived reopen; unknown error retains validated attempt; replay changed=%t last=%t; joined readers/writers=0/0", replay.Changed, replay.LastForRoot)
}

func TestRemoteResolutionUnknownDuplicateHasNoAttempt(t *testing.T) {
	db, store, _, _, link := remoteSettlementFixture(t)
	if result, err := store.ResolveRemoteRun(t.Context(), link, model.RunRetentionPolicy{}); err != nil || !result.Changed {
		t.Fatalf("initial release: %+v %v", result, err)
	}
	injected := engine.InjectCommitAfterForModuleTest(t, db)
	result, err := store.ResolveRemoteRun(t.Context(), link, model.RunRetentionPolicy{})
	remoteUnknown(t, err, false, true)
	if !errors.Is(err, injected) || result != (model.RemoteRunResolveResult{}) {
		t.Fatalf("duplicate fabricated an attempted deletion: %+v %v", result, err)
	}
	var resolution *coredata.RemoteRunResolveError
	if !errors.As(err, &resolution) {
		t.Fatal(err)
	}
	fact, err := resolution.WaitRevalidation(t.Context())
	if err != nil || fact.Released || fact.LastForOwner || fact.LastForRoot || fact.Retention != (model.RunRetentionEffect{}) {
		t.Fatalf("duplicate manufactured a release: %+v %v", fact, err)
	}
}

func TestRemoteResolutionWrongBootRollbackFailureHasNoAttempt(t *testing.T) {
	db, store, _, _, link := remoteSettlementFixture(t)
	injected := engine.InjectRollbackFailureForModuleTest(t, db)
	wrong := link
	wrong.BootID = "wrong-boot"
	result, err := store.ResolveRemoteRun(t.Context(), wrong, model.RunRetentionPolicy{})
	remoteUnknown(t, err, false, true)
	if !errors.Is(err, injected) || result != (model.RemoteRunResolveResult{}) {
		t.Fatalf("wrong identity fabricated an attempted deletion: %+v %v", result, err)
	}
	var resolution *coredata.RemoteRunResolveError
	if !errors.As(err, &resolution) {
		t.Fatal(err)
	}
	fact, e := resolution.WaitRevalidation(t.Context())
	if e != nil || !fact.NonCommitted || fact.ValidatedAttempt || fact.Released {
		t.Fatalf("wrong identity rollback fact: %+v %v", fact, e)
	}
	if current, e := store.ReadRemoteRunLink(t.Context(), link); e != nil || current.Phase != model.RemoteRunReleased {
		t.Fatalf("wrong identity removed the exact released relation: %+v %v", current, e)
	}
}

func TestRemoteResolutionFailedDeleteAttemptDoesNotAssertRelease(t *testing.T) {
	db, store, _, _, link := remoteSettlementFixture(t)
	if err := db.Write(t.Context(), engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TRIGGER injected_delete_failure BEFORE DELETE ON run_remote_pending BEGIN SELECT RAISE(ABORT,'injected delete failure'); END`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	injected := engine.InjectRollbackFailureForModuleTest(t, db)
	result, err := store.ResolveRemoteRun(t.Context(), link, model.RunRetentionPolicy{})
	remoteUnknown(t, err, true, true)
	var resolution *coredata.RemoteRunResolveError
	if !errors.As(err, &resolution) || resolution.AttemptedLink != link {
		t.Fatalf("failed DELETE lost its exact attempted identity: %+v %v", resolution, err)
	}
	if !errors.Is(err, injected) || result != (model.RemoteRunResolveResult{}) {
		t.Fatalf("failed DELETE was reported as a release: %+v %v", result, err)
	}
	fact, waitErr := resolution.WaitRevalidation(t.Context())
	if waitErr != nil || !fact.NonCommitted || !fact.ValidatedAttempt || fact.Released {
		t.Fatalf("failed DELETE rollback fact: %+v %v", fact, waitErr)
	}
	if current, err := store.ReadRemoteRunLink(t.Context(), link); err != nil || current.Phase != model.RemoteRunReleased {
		t.Fatalf("failed DELETE lost the exact relation: %+v %v", current, err)
	}
	if err := db.Write(t.Context(), engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DROP TRIGGER injected_delete_failure`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if result, err := store.ResolveRemoteRun(t.Context(), link, model.RunRetentionPolicy{}); err != nil || !result.Changed {
		t.Fatalf("normal resolution after joined rollback: %+v %v", result, err)
	}
}

func TestRemoteResolutionUnjoinedRollbackFencesRevalidation(t *testing.T) {
	db, store, cfg, scope, link := remoteSettlementFixture(t)
	injected := engine.InjectUnjoinedRollbackForModuleTest(t, db)
	wrong := link
	wrong.BootID = "wrong-boot"
	_, err := store.ResolveRemoteRun(t.Context(), wrong, model.RunRetentionPolicy{})
	remoteUnknown(t, err, false, false)
	if !errors.Is(err, injected) {
		t.Fatalf("lost rollback cause: %v", err)
	}
	if pending, err := store.HasPendingRemoteRoot(t.Context(), link.RootRunID); err == nil || pending {
		t.Fatalf("unjoined owner supplied a revalidation fact: pending=%v error=%v", pending, err)
	}
	if err = db.Close(); err != nil {
		t.Fatalf("explicit physical join: %v", err)
	}
	cfg.Create = false
	reopened, err := engine.Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	store, err = coredata.New(reopened, scope)
	if err != nil {
		t.Fatal(err)
	}
	if retained, err := store.ReadRemoteRunLink(t.Context(), link); err != nil || retained.Phase != model.RemoteRunReleased {
		t.Fatalf("joined reopen lost exact relation: %+v error=%v", retained, err)
	}
}

func TestRemoteResolutionValidationAndNormalOwnerWork(t *testing.T) {
	db, store, _, _, link := remoteSettlementFixture(t)
	bad := link
	bad.CallNodeID = ""
	before, _ := db.Stats()
	if _, err := store.ResolveRemoteRun(t.Context(), bad, model.RunRetentionPolicy{}); !engine.DefinitelyNotCommitted(err) {
		t.Fatalf("invalid identity was not a definite pre-effect refusal: %v", err)
	}
	afterRefusal, _ := db.Stats()
	if afterRefusal.Calls != before.Calls {
		t.Fatalf("invalid identity entered the owner: %+v %+v", before, afterRefusal)
	}
	result, err := store.ResolveRemoteRun(context.Background(), link, model.RunRetentionPolicy{})
	if err != nil || !result.Changed || !result.LastForRoot {
		t.Fatalf("normal release: %+v %v", result, err)
	}
	after, _ := db.Stats()
	if after.Calls-afterRefusal.Calls != 1 || after.CommitsDurable-afterRefusal.CommitsDurable != 1 || after.WritersQueued != 0 || after.ReadersActive != 0 {
		t.Fatalf("normal release changed work shape: before=%+v after=%+v", afterRefusal, after)
	}
	t.Logf("normal release: +1 owner call, +1 durable, no extra reader, joined readers/writers=0/0")
}

func TestNestedOwnerRecoveryNoticeSurvivesLostResolutionReply(t *testing.T) {
	db, store, _, _, _ := remoteSettlementFixture(t)
	ctx := t.Context()
	parent := model.NewRun{ID: "nested-parent", TargetID: "agent-parent", RootRunID: "root", ParentRunID: "root", CallNodeID: "outer", Depth: 1,
		WorkflowResourceID: "wf", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`),
		IncomingHold: model.IncomingRemoteHold{TargetID: "core", PrincipalID: "core-principal"}, RecoveryAgentBootID: "old-parent-boot"}
	if _, _, err := store.CreateRun(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if err := store.AdmitRunResource(ctx, parent.ID, parent.RecoveryAgentBootID, model.RunResourceAdmission{ID: "call-admission", NodeID: "call", Kind: "workflow-call"}); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: parent.ID, RootRunID: "root", TargetID: "agent-grandchild", RunID: "nested-child", CallNodeID: "call", BootID: "old-grandchild-boot"}
	if err := store.ReserveRemoteRun(ctx, link); err != nil {
		t.Fatal(err)
	}
	if _, err := store.InterruptOpenRuns(ctx, time.Now().UTC(), model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SettleRemoteRun(ctx, link, model.RemoteRunSettlement{Disposition: "canceled", FinishedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRemoteRunReleased(ctx, link, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	engine.InjectCommitAfterForModuleTest(t, db)
	if _, err := store.ResolveRemoteRun(ctx, link, model.RunRetentionPolicy{}); err == nil {
		t.Fatal("expected a lost reply after the real commit")
	} else {
		var fault *coredata.RemoteRunResolveError
		if !errors.As(err, &fault) {
			t.Fatalf("missing exact resolution fault: %v", err)
		}
		fact, err := fault.WaitRevalidation(ctx)
		if err != nil || !fact.ValidatedAttempt || !fact.Released || fact.RecoveredOwner == nil || fact.RecoveredOwner.ID != parent.ID || fact.RecoveredOwner.Status != model.RunInterrupted {
			t.Fatalf("lost reply discarded the exact owner notification: %+v %v", fact, err)
		}
	}
	state, found, err := store.GetRunResourceRecovery(ctx, parent.ID)
	if err != nil || !found || !state.Joined {
		t.Fatalf("actual parent receipt was not durable: %+v %v %v", state, found, err)
	}
}
