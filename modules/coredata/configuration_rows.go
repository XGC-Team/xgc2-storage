package coredata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"strconv"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func configurationResource(ctx context.Context, tx *sql.Tx, scope, domain, id string, includeArchived bool) (out model.ConfigurationResource, err error) {
	var body []byte
	var row model.ConfigurationResource
	var archived bool
	err = tx.QueryRowContext(ctx, "SELECT id,coalesce(namespace_id,''),name,name_key,revision,main_commit_id,archived,origin_resource_id,origin_commit_id,body FROM core_resources WHERE scope=? AND domain=? AND id=?", scope, domain, id).Scan(&row.ID, &row.NamespaceID, &row.Name, &row.NameKey, &row.Revision, &row.MainCommitID, &archived, &row.OriginResourceID, &row.OriginCommitID, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return out, failure("not_found", "resource not found")
	}
	if err != nil {
		return out, err
	}
	if json.Unmarshal(body, &out) != nil || out.ID != row.ID || out.NamespaceID != row.NamespaceID || out.Name != row.Name || out.NameKey != row.NameKey || out.Revision != row.Revision || out.MainCommitID != row.MainCommitID || out.OriginResourceID != row.OriginResourceID || out.OriginCommitID != row.OriginCommitID || !positiveRevision(out.NextVersion) || archived != (out.ArchivedAt != "") {
		return out, failure("data_loss", "resource metadata disagrees with relational identity")
	}
	if archived && !includeArchived {
		return out, failure("not_found", "live resource not found")
	}
	return out, nil
}
func configurationBranch(ctx context.Context, tx *sql.Tx, scope, domain, id string) (out model.ConfigurationBranch, err error) {
	var row model.ConfigurationBranch
	var body []byte
	err = tx.QueryRowContext(ctx, "SELECT id,resource_id,name_key,head_commit_id,revision,body FROM core_branches WHERE scope=? AND domain=? AND id=?", scope, domain, id).Scan(&row.ID, &row.ResourceID, &row.NameKey, &row.HeadCommitID, &row.Revision, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return out, failure("not_found", "branch not found")
	}
	if err != nil {
		return out, err
	}
	if json.Unmarshal(body, &out) != nil || out.ID != row.ID || out.ResourceID != row.ResourceID || out.NameKey != row.NameKey || out.HeadCommitID != row.HeadCommitID || out.Revision != row.Revision || !positiveRevision(out.Revision) {
		return out, failure("data_loss", "branch metadata disagrees with relational identity")
	}
	return out, nil
}
func configurationBranchNamed(ctx context.Context, tx *sql.Tx, scope, domain, resource, name string) (model.ConfigurationBranch, error) {
	var id string
	err := tx.QueryRowContext(ctx, "SELECT id FROM core_branches WHERE scope=? AND domain=? AND resource_id=? AND name_key=?", scope, domain, resource, name).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return model.ConfigurationBranch{}, failure("not_found", "branch not found")
	}
	if err != nil {
		return model.ConfigurationBranch{}, err
	}
	return configurationBranch(ctx, tx, scope, domain, id)
}
func configurationCommitRow(ctx context.Context, tx *sql.Tx, scope, domain, resource, id string) (out model.ConfigurationResourceSnapshot, err error) {
	var row model.ConfigurationCommit
	var body []byte
	var branchResource sql.NullString
	// A snapshot's resource FK and branch FK are independent. Bind the owning
	// branch to that same aggregate in this indexed point read. Forked branches
	// may share a head; its original branch need not be the selected live branch.
	err = tx.QueryRowContext(ctx, `SELECT s.id,s.resource_id,s.branch_id,s.version,s.source_commit_id,s.content_digest,s.payload,s.manifest,s.body,b.resource_id
 FROM core_snapshots s LEFT JOIN core_branches b ON b.scope=s.scope AND b.domain=s.domain AND b.id=s.branch_id
 WHERE s.scope=? AND s.domain=? AND s.id=? AND s.resource_id=?`, scope, domain, id, resource).Scan(&row.ID, &row.ResourceID, &row.BranchID, &row.Version, &row.SourceCommitID, &row.ContentDigest, &out.Payload, &out.Manifest, &body, &branchResource)
	if errors.Is(err, sql.ErrNoRows) {
		return out, failure("not_found", "owned immutable commit not found")
	}
	if err != nil {
		return out, err
	}
	if !branchResource.Valid || branchResource.String != row.ResourceID {
		return out, failure("data_loss", "immutable snapshot branch belongs to another resource")
	}
	if json.Unmarshal(body, &out.Head.Commit) != nil || out.Head.Commit.ID != row.ID || out.Head.Commit.ResourceID != row.ResourceID || out.Head.Commit.BranchID != row.BranchID || out.Head.Commit.Version != row.Version || out.Head.Commit.SourceCommitID != row.SourceCommitID || out.Head.Commit.ContentDigest != row.ContentDigest || !positiveRevision(row.Version) || out.Head.Commit.SchemaVersion < 1 || !canonicalSessionPin(out.Head.Commit.RootDigest) {
		return out, failure("data_loss", "immutable commit metadata disagrees")
	}
	rows, err := tx.QueryContext(ctx, "SELECT slot,target_domain,target_resource_id,target_commit_id,body FROM core_references WHERE scope=? AND domain=? AND commit_id=? ORDER BY slot LIMIT 4097", scope, domain, id)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	out.References = []model.ConfigurationReference{}
	size := len(out.Payload) + len(out.Manifest) + len(body)
	for rows.Next() {
		var ref model.ConfigurationReference
		var slot, td, tr, tc string
		var b []byte
		if err = rows.Scan(&slot, &td, &tr, &tc, &b); err != nil {
			return out, err
		}
		size += len(b)
		if size > model.MaxConfigurationDecodedBytes || len(out.References) >= model.MaxConfigurationReferences {
			return out, failure("resource_exhausted", "immutable snapshot materialization bound exceeded")
		}
		if json.Unmarshal(b, &ref) != nil || ref.Slot != slot || ref.TargetDomain != td || ref.TargetResourceID != tr || ref.TargetCommitID != tc {
			return out, failure("data_loss", "reference metadata disagrees")
		}
		out.References = append(out.References, ref)
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	pin, e := model.ConfigurationContentDigest(out.Payload, out.Manifest, out.References)
	if e != nil || pin != row.ContentDigest {
		return out, failure("data_loss", "immutable content digest mismatch")
	}
	return out, nil
}
func configurationIdentity(payload []byte) ([]byte, error) {
	if len(payload) > model.MaxConfigurationPayloadBytes {
		return nil, failure("resource_exhausted", "frozen payload exceeds bound")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	start, e := decoder.Token()
	if e != nil || start != json.Delim('{') {
		return nil, failure("invalid_argument", "generic identity/body envelope required")
	}
	envelope := map[string]json.RawMessage{}
	for decoder.More() {
		token, e := decoder.Token()
		key, ok := token.(string)
		if e != nil || !ok || (key != "identity" && key != "body") || envelope[key] != nil {
			return nil, failure("invalid_argument", "exactly one identity and body required")
		}
		var raw json.RawMessage
		if decoder.Decode(&raw) != nil || !object(raw) {
			return nil, failure("invalid_argument", "generic envelope members must be objects")
		}
		envelope[key] = raw
	}
	end, e := decoder.Token()
	var tail any
	if e != nil || end != json.Delim('}') || len(envelope) != 2 || decoder.Decode(&tail) != io.EOF {
		return nil, failure("invalid_argument", "one complete generic identity/body envelope required")
	}
	if len(envelope["identity"]) > model.MaxConfigurationIdentityBytes {
		return nil, failure("resource_exhausted", "generic identity projection exceeds16KiB")
	}
	return envelope["identity"], nil
}
func configurationCurrentMain(ctx context.Context, tx *sql.Tx, scope, domain string, r model.ConfigurationResource, selected *model.ConfigurationResourceSnapshot) (out model.ConfigurationVisibilityPin, err error) {
	b, err := configurationBranchNamed(ctx, tx, scope, domain, r.ID, "main")
	if err != nil {
		return out, err
	}
	if b.ArchivedAt != "" || b.HeadCommitID != r.MainCommitID {
		return out, failure("data_loss", "current main relation disagrees")
	}
	var commit model.ConfigurationCommit
	var payload []byte
	var identity []byte
	if selected != nil && selected.Head.Commit.ID == r.MainCommitID {
		commit = selected.Head.Commit
		payload = selected.Payload
	} else {
		var body []byte
		var branchID, contentDigest string
		var branchResource sql.NullString
		// Project only the generic member from the authoritative blob. Do not
		// materialize a second full opaque payload for historical/nonmain reads.
		err = tx.QueryRowContext(ctx, `SELECT s.body,json_extract(CAST(s.payload AS TEXT),'$.identity'),s.branch_id,s.content_digest,b.resource_id
 FROM core_snapshots s LEFT JOIN core_branches b ON b.scope=s.scope AND b.domain=s.domain AND b.id=s.branch_id
 WHERE s.scope=? AND s.domain=? AND s.resource_id=? AND s.id=?`, scope, domain, r.ID, r.MainCommitID).Scan(&body, &identity, &branchID, &contentDigest, &branchResource)
		if errors.Is(err, sql.ErrNoRows) {
			return out, failure("data_loss", "current main commit is missing")
		}
		if err != nil {
			return out, err
		}
		if json.Unmarshal(body, &commit) != nil || !branchResource.Valid || branchResource.String != r.ID || commit.BranchID != branchID || commit.ContentDigest != contentDigest {
			return out, failure("data_loss", "current main metadata disagrees")
		}
		if !object(identity) || len(identity) > model.MaxConfigurationIdentityBytes {
			return out, failure("data_loss", "invalid bounded current-main identity projection")
		}
	}
	if commit.ID != r.MainCommitID || commit.ResourceID != r.ID || commit.BranchID != b.ID || commit.SchemaVersion < 1 || !canonicalSessionPin(commit.ContentDigest) {
		return out, failure("data_loss", "current main metadata disagrees")
	}
	if identity == nil {
		var e error
		identity, e = configurationIdentity(payload)
		if e != nil {
			return out, e
		}
	}
	out = model.ConfigurationVisibilityPin{Branch: model.ConfigurationBranchGuard{ID: b.ID, ExpectedRevision: b.Revision, CommitID: commit.ID, ContentDigest: commit.ContentDigest}, SchemaVersion: commit.SchemaVersion, Identity: identity}
	return out, nil
}
func configurationRead(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationResourceRead) (out model.ConfigurationResourceSnapshot, err error) {
	if _, err = configurationDomain(ctx, tx, q.Domain); err != nil {
		return out, err
	}
	var r model.ConfigurationResource
	var b model.ConfigurationBranch
	var commitID string
	if q.ResourceID != "" {
		if !textKey(q.ResourceID) || q.NamespaceID != "" || q.NameKey != "" || (q.Branch == "") == (q.CommitID == "") {
			return out, failure("invalid_argument", "exact resource+branch or resource+commit selector required")
		}
		r, err = configurationResource(ctx, tx, scope, q.Domain.Key, q.ResourceID, q.IncludeArchived)
		if err != nil {
			return out, err
		}
		if q.CommitID != "" {
			if !textKey(q.CommitID) {
				return out, failure("invalid_argument", "invalid commit selector")
			}
			commitID = q.CommitID
		} else {
			_, key, e := model.NormalizeConfigurationName(model.ConfigurationBranchName, q.Branch)
			if e != nil || key != q.Branch {
				return out, failure("invalid_argument", "canonical bounded branch selector required")
			}
			b, err = configurationBranchNamed(ctx, tx, scope, q.Domain.Key, r.ID, q.Branch)
			if err != nil {
				return out, err
			}
			if b.ArchivedAt != "" && !q.IncludeArchived {
				return out, failure("not_found", "live branch not found")
			}
			commitID = b.HeadCommitID
		}
	} else {
		if q.Branch != "" || q.CommitID != "" || model.ValidateConfigurationName(model.ConfigurationResourceName, q.NameKey) != nil || q.NamespaceID != "" && !textKey(q.NamespaceID) {
			return out, failure("invalid_argument", "exact namespace+name selector required")
		}
		var id string
		err = tx.QueryRowContext(ctx, "SELECT id FROM core_resources WHERE scope=? AND domain=? AND coalesce(namespace_id,'')=? AND name_key=? AND archived=0", scope, q.Domain.Key, q.NamespaceID, q.NameKey).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return out, failure("not_found", "named resource not found")
		}
		if err != nil {
			return out, err
		}
		r, err = configurationResource(ctx, tx, scope, q.Domain.Key, id, false)
		if err != nil {
			return out, err
		}
		b, err = configurationBranchNamed(ctx, tx, scope, q.Domain.Key, id, "main")
		if err != nil {
			return out, err
		}
		commitID = b.HeadCommitID
	}
	out, err = configurationCommitRow(ctx, tx, scope, q.Domain.Key, r.ID, commitID)
	if err != nil {
		return out, err
	}
	if b.ID == "" {
		b, err = configurationBranch(ctx, tx, scope, q.Domain.Key, out.Head.Commit.BranchID)
		if err != nil {
			return out, err
		}
	}
	if b.ResourceID != r.ID {
		return out, failure("data_loss", "selected branch ownership disagrees")
	}
	out.Head.Resource = r
	out.Head.Branch = b
	if _, err = configurationIdentity(out.Payload); err != nil {
		return out, err
	}
	out.CurrentMain, err = configurationCurrentMain(ctx, tx, scope, q.Domain.Key, r, &out)
	if err != nil {
		return out, err
	}
	manifest, e := model.DecodeConfigurationManifest(out.Manifest)
	if e != nil || manifest.RootDigest != out.Head.Commit.RootDigest {
		return out, failure("data_loss", "stored generic manifest disagrees")
	}
	metadata := out
	metadata.Payload, metadata.Manifest, metadata.CurrentMain.Identity = nil, nil, nil
	body, e := encode(metadata)
	if e != nil {
		return out, e
	}
	result, e := encode(out)
	if e != nil {
		return out, e
	}
	wire, e := json.Marshal(api.NamedResponse{Result: result})
	if e != nil {
		return out, e
	}
	budget := model.ConfigurationBudget{PayloadBytes: int64(len(out.Payload)), ManifestBytes: int64(len(out.Manifest)), ManifestNodes: int64(len(manifest.Nodes)), References: int64(len(out.References)), MainIdentityBytes: int64(len(out.CurrentMain.Identity)), DecodedBytes: int64(len(out.Payload) + len(out.Manifest) + len(body) + len(out.CurrentMain.Identity)), WireBytes: int64(len(wire))}
	if err = budget.Validate(); err != nil {
		return out, failure("resource_exhausted", err.Error())
	}
	return out, nil
}
func configurationNamespaceGuard(ctx context.Context, tx *sql.Tx, scope, domain string, g model.ConfigurationNamespaceGuard) error {
	if g.ID == "" {
		if g.ExpectedRevision != "0" {
			return failure("invalid_argument", "structural root guard must be empty/0")
		}
		return nil
	}
	if !textKey(g.ID) || !positiveRevision(g.ExpectedRevision) {
		return failure("invalid_argument", "invalid namespace point guard")
	}
	var revision string
	err := tx.QueryRowContext(ctx, "SELECT revision FROM core_namespaces WHERE scope=? AND domain=? AND id=? AND archived=0", scope, domain, g.ID).Scan(&revision)
	if errors.Is(err, sql.ErrNoRows) || err == nil && revision != g.ExpectedRevision {
		return failure("conflict", "namespace point changed")
	}
	return err
}
func configurationReceipt(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationReceipt) (out model.ConfigurationMutationResult, err error) {
	if !textKey(q.Domain.Key) || !textKey(q.Domain.SchemaIdentity) || q.Domain.SchemaVersion < 1 || !canonicalSessionPin(q.Domain.RegistryDigest) || !textKey(q.Key) || !canonicalSessionPin(q.IntentDigest) || len(q.Operations) < 1 || len(q.Operations) > 2 {
		return out, failure("invalid_argument", "product receipt identity/allowed operation required")
	}
	for i, op := range q.Operations {
		if op != model.ResourceCreateOperation && op != model.ResourceCommitOperation || i > 0 && op == q.Operations[0] {
			return out, failure("invalid_argument", "invalid product receipt operation")
		}
	}
	// Current accepted deployment catalog is an admission fence, including
	// historical receipt lookup. A matching new catalog may recover old facts;
	// a stale catalog never bypasses admission through a durable receipt.
	if _, err = configurationDomain(ctx, tx, q.Domain); err != nil {
		return out, err
	}
	var intent, op string
	var body []byte
	err = tx.QueryRowContext(ctx, `SELECT intent_digest,operation,body FROM core_configuration_receipts WHERE scope=? AND domain=? AND mutation_key=? UNION ALL SELECT intent_digest,operation,body FROM core_catalog_receipts WHERE scope=? AND domain=? AND mutation_key=?`, scope, q.Domain.Key, q.Key, scope, q.Domain.Key, q.Key).Scan(&intent, &op, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return model.ConfigurationMutationResult{Key: q.Key, Domain: q.Domain.Key, IntentDigest: q.IntentDigest}, nil
	}
	if err != nil {
		return out, err
	}
	allowed := false
	for _, candidate := range q.Operations {
		allowed = allowed || candidate == op
	}
	if intent != q.IntentDigest || !allowed {
		return out, failure("conflict", "product mutation identity reused")
	}
	if len(body) > model.MaxConfigurationReceiptResultBytes || json.Unmarshal(body, &out) != nil || out.Key != q.Key || out.Domain != q.Domain.Key || out.Operation != op || out.IntentDigest != intent || !out.Found {
		return out, failure("data_loss", "invalid durable product receipt")
	}
	out.Replayed = true
	return out, nil
}
func configurationSaveReceipt(ctx context.Context, tx *sql.Tx, scope string, out model.ConfigurationMutationResult) error {
	body, err := encode(out)
	if err != nil {
		return err
	}
	if len(body) > model.MaxConfigurationReceiptResultBytes {
		return failure("resource_exhausted", "compact product receipt bound exceeded")
	}
	if err = reserve(ctx, tx, scope, 1, int64(len(body))); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO core_configuration_receipts VALUES(?,?,?,?,?,?,?)", scope, out.Domain, out.Key, out.IntentDigest, out.Operation, out.PlanDigest, body)
	return err
}
func configurationBump(s string) (string, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 1 || n == int64(^uint64(0)>>1) {
		return "", failure("resource_exhausted", "canonical revision/version overflow")
	}
	return strconv.FormatInt(n+1, 10), nil
}
func configurationNullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
