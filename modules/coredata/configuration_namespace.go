package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"math"
	"strconv"
	"time"
)

func configurationNamespace(ctx context.Context, tx *sql.Tx, scope, domain, id string) (out model.ConfigurationNamespace, body []byte, err error) {
	var parent, name, key, revision string
	var archived bool
	err = tx.QueryRowContext(ctx, "SELECT coalesce(parent_id,''),name,name_key,revision,archived,body FROM core_namespaces WHERE scope=? AND domain=? AND id=?", scope, domain, id).Scan(&parent, &name, &key, &revision, &archived, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return out, nil, failure("not_found", "namespace not found")
	}
	if err != nil {
		return out, nil, err
	}
	if json.Unmarshal(body, &out) != nil || out.ID != id || out.ParentID != parent || out.Name != name || out.NameKey != key || out.Revision != revision || !positiveRevision(revision) || archived != (out.ArchivedAt != "") {
		return out, nil, failure("data_loss", "namespace metadata disagrees with owner row")
	}
	return
}

func configurationNamespaceWrite(ctx context.Context, tx *sql.Tx, scope, operation string, q model.ConfigurationNamespaceWrite) (out model.ConfigurationNamespaceResult, err error) {
	if _, err = configurationDomain(ctx, tx, q.Domain); err != nil {
		return out, err
	}
	if err = configurationMutationValid(q.Mutation); err != nil {
		return out, err
	}
	if !textKey(q.ID) {
		return out, failure("invalid_argument", "namespace identity required")
	}
	var intent, op string
	var receipt []byte
	// Both current receipt tables share the product mutation identity. This is
	// one indexed read, not a legacy lookup or an automatic transport retry.
	err = tx.QueryRowContext(ctx, `SELECT intent_digest,operation,body FROM core_catalog_receipts WHERE scope=? AND domain=? AND mutation_key=?
 UNION ALL SELECT intent_digest,operation,body FROM core_configuration_receipts WHERE scope=? AND domain=? AND mutation_key=?`, scope, q.Domain.Key, q.Mutation.Key, scope, q.Domain.Key, q.Mutation.Key).Scan(&intent, &op, &receipt)
	if err == nil {
		if intent != q.Mutation.IntentDigest || op != operation {
			return out, failure("conflict", "product mutation identity reused")
		}
		if json.Unmarshal(receipt, &out) != nil {
			return out, failure("data_loss", "invalid namespace mutation receipt")
		}
		out.Replayed = true
		return out, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var oldBody []byte
	if operation == model.ConfigurationNamespaceCreateOperation {
		if q.ExpectedRevision != "0" || q.Archived || q.Parent == nil {
			return out, failure("invalid_argument", "create requires explicit parent and zero revision")
		}
		out.Namespace = model.ConfigurationNamespace{ID: q.ID, Revision: "1", CreatedAt: now, UpdatedAt: now}
	} else {
		if !positiveRevision(q.ExpectedRevision) {
			return out, failure("invalid_argument", "expected namespace revision required")
		}
		out.Namespace, oldBody, err = configurationNamespace(ctx, tx, scope, q.Domain.Key, q.ID)
		if err != nil {
			return out, err
		}
		if out.Namespace.Revision != q.ExpectedRevision {
			return out, failure("conflict", "namespace revision changed")
		}
	}
	n := &out.Namespace
	unchanged := false
	switch operation {
	case model.ConfigurationNamespaceCreateOperation, model.ConfigurationNamespaceUpdateOperation:
		if n.ArchivedAt != "" || q.Archived {
			return out, failure("failed_precondition", "live namespace required for editing")
		}
		if q.Name != "" || operation == model.ConfigurationNamespaceCreateOperation {
			n.Name, n.NameKey, err = model.NormalizeConfigurationName(model.ConfigurationNamespaceName, q.Name)
			if err != nil {
				return out, failure("invalid_argument", err.Error())
			}
		}
		if q.Parent != nil {
			if err = configurationNamespaceGuard(ctx, tx, scope, q.Domain.Key, *q.Parent); err != nil {
				return out, err
			}
			n.ParentID = q.Parent.ID
		}
		// A finite ancestor walk prevents self/descendant moves while retaining
		// flat large catalogs. The owner transaction fences the checked topology.
		rows, e := tx.QueryContext(ctx, `WITH RECURSIVE parents(id,parent_id) AS (
    SELECT id,parent_id FROM core_namespaces WHERE scope=? AND domain=? AND id=?
    UNION SELECT n.id,n.parent_id FROM core_namespaces n JOIN parents p ON n.id=p.parent_id WHERE n.scope=? AND n.domain=? LIMIT 129
   ) SELECT id FROM parents`, scope, q.Domain.Key, n.ParentID, scope, q.Domain.Key)
		if e != nil {
			return out, e
		}
		count := 0
		for rows.Next() {
			var id string
			if e = rows.Scan(&id); e != nil {
				break
			}
			count++
			if id == n.ID {
				e = failure("failed_precondition", "namespace parent cycle")
				break
			}
		}
		if e == nil {
			e = rows.Err()
		}
		rows.Close()
		if e != nil {
			return out, e
		}
		if count >= 129 {
			return out, failure("resource_exhausted", "namespace depth exceeds bound")
		}
	case model.ConfigurationNamespaceStateOperation:
		if q.Parent != nil || q.Name != "" {
			return out, failure("invalid_argument", "state change cannot edit namespace identity")
		}
		if q.Archived && n.ArchivedAt == "" {
			var busy bool
			err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM core_namespaces WHERE scope=? AND domain=? AND parent_id=? AND archived=0)
    OR EXISTS(SELECT 1 FROM core_resources WHERE scope=? AND domain=? AND namespace_id=? AND archived=0)`, scope, q.Domain.Key, n.ID, scope, q.Domain.Key, n.ID).Scan(&busy)
			if err != nil {
				return out, err
			}
			if busy {
				return out, failure("failed_precondition", "namespace is not empty")
			}
			n.ArchivedAt = now
		} else if !q.Archived && n.ArchivedAt != "" {
			if n.ParentID != "" {
				var live bool
				err = tx.QueryRowContext(ctx, "SELECT archived=0 FROM core_namespaces WHERE scope=? AND domain=? AND id=?", scope, q.Domain.Key, n.ParentID).Scan(&live)
				if err != nil {
					return out, err
				}
				if !live {
					return out, failure("failed_precondition", "namespace parent is archived")
				}
			}
			n.ArchivedAt = ""
		} else {
			unchanged = true
		}
	default:
		return out, failure("invalid_argument", "unregistered namespace mutation")
	}
	if operation != model.ConfigurationNamespaceCreateOperation && !unchanged {
		previous, _ := strconv.ParseInt(n.Revision, 10, 64)
		if previous == math.MaxInt64 {
			return out, failure("resource_exhausted", "namespace revision exhausted")
		}
		n.Revision = strconv.FormatInt(previous+1, 10)
		n.UpdatedAt = now
	}
	if !unchanged && n.ArchivedAt == "" {
		var conflict bool
		err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM core_namespaces WHERE scope=? AND domain=? AND coalesce(parent_id,'')=? AND name_key=? AND archived=0 AND id<>?)", scope, q.Domain.Key, n.ParentID, n.NameKey, n.ID).Scan(&conflict)
		if err != nil {
			return out, err
		}
		if conflict {
			return out, failure("conflict", "namespace name is already in use")
		}
	}
	body, e := encode(n)
	if e != nil {
		return out, e
	}
	archived := n.ArchivedAt != ""
	if unchanged {
		// The durable receipt is new; namespace state and revision stay intact.
	} else if operation == model.ConfigurationNamespaceCreateOperation {
		if err = reserve(ctx, tx, scope, 1, int64(len(body))); err != nil {
			return out, err
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO core_namespaces VALUES(?,?,?,nullif(?,''),?,?,?, ?,?)", scope, q.Domain.Key, n.ID, n.ParentID, n.Name, n.NameKey, n.Revision, archived, body)
	} else {
		if err = reserve(ctx, tx, scope, 0, int64(len(body)-len(oldBody))); err != nil {
			return out, err
		}
		var result sql.Result
		result, err = tx.ExecContext(ctx, "UPDATE core_namespaces SET parent_id=nullif(?,''),name=?,name_key=?,revision=?,archived=?,body=? WHERE scope=? AND domain=? AND id=? AND revision=?", n.ParentID, n.Name, n.NameKey, n.Revision, archived, body, scope, q.Domain.Key, n.ID, q.ExpectedRevision)
		if err == nil {
			var changed int64
			changed, err = result.RowsAffected()
			if err == nil && changed != 1 {
				err = failure("conflict", "namespace revision changed")
			}
		}
	}
	if err != nil {
		return out, err
	}
	if !unchanged {
		change := model.ConfigurationChange{ID: digest(struct{ Domain, Key string }{q.Domain.Key, q.Mutation.Key}), Summary: operation}
		if err = configurationChange(ctx, tx, scope, q.Domain.Key, n.ID, "", operation, q.Mutation, change, "", n.Name); err != nil {
			return out, err
		}
	}
	receipt, err = encode(out)
	if err != nil {
		return out, err
	}
	if err = reserve(ctx, tx, scope, 1, int64(len(receipt))); err != nil {
		return out, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO core_catalog_receipts VALUES(?,?,?,?,?,?)", scope, q.Domain.Key, q.Mutation.Key, q.Mutation.IntentDigest, operation, receipt)
	return out, err
}
