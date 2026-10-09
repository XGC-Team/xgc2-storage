package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func configurationIncomingRead(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationIncomingRead) (out []model.ConfigurationIncomingReference, err error) {
	if _, err = configurationDomain(ctx, tx, q.Domain); err != nil {
		return
	}
	if !textKey(q.ResourceID) {
		return nil, failure("invalid_argument", "target resource identity required")
	}
	rows, err := tx.QueryContext(ctx, `SELECT s.domain,s.resource_id,b.name_key,s.id,f.body,coalesce(json_extract(CAST(s.body AS TEXT),'$.created_at'),'') FROM core_references f
 JOIN core_snapshots s ON s.scope=f.scope AND s.domain=f.domain AND s.id=f.commit_id
 JOIN core_resources r ON r.scope=s.scope AND r.domain=s.domain AND r.id=s.resource_id
 JOIN core_branches b ON b.scope=s.scope AND b.domain=s.domain AND b.resource_id=s.resource_id AND b.head_commit_id=s.id
 WHERE f.scope=? AND f.target_domain=? AND f.target_resource_id=? AND r.archived=0
 AND coalesce(json_extract(CAST(b.body AS TEXT),'$.archived_at'),'')=''
 ORDER BY s.domain,s.resource_id,b.name_key,f.slot LIMIT 16385`, scope, q.Domain.Key, q.ResourceID)
	if err != nil {
		return
	}
	defer rows.Close()
	size := 0
	out = []model.ConfigurationIncomingReference{}
	for rows.Next() {
		var v model.ConfigurationIncomingReference
		var raw []byte
		if err = rows.Scan(&v.SourceDomain, &v.SourceResourceID, &v.SourceBranch, &v.SourceCommitID, &raw, &v.CreatedAt); err != nil {
			return nil, err
		}
		size += len(raw)
		if len(out) == MaxReferences || size > model.MaxConfigurationDecodedBytes {
			return nil, failure("resource_exhausted", "incoming references exceed bound")
		}
		if json.Unmarshal(raw, &v.Reference) != nil {
			return nil, failure("data_loss", "invalid incoming reference")
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
