package coredata

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// ReadSystemResource finds one protected provisioned identity using the unique
// system-key index. It reads no frozen snapshot. A caller exposing a typed
// resource must still perform the selected resource's own visibility admission.
func (s *Store) ReadSystemResource(ctx context.Context, domain model.ConfigurationDomainGuard, key string) (model.ConfigurationResource, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationResource, error) {
		return configurationSystemResource(ctx, tx, scope, domain, key)
	})
}

const configurationSystemResourceQuery = `SELECT id,coalesce(namespace_id,''),name,name_key,revision,main_commit_id,archived,origin_resource_id,origin_commit_id,body
FROM core_resources INDEXED BY core_resource_system_keys
WHERE scope=? AND domain=? AND json_extract(CAST(body AS TEXT),'$.system')=1
AND json_extract(CAST(body AS TEXT),'$.system_key')<>'' AND json_extract(CAST(body AS TEXT),'$.system_key')=?`

func configurationSystemResource(ctx context.Context, tx *sql.Tx, scope string, domain model.ConfigurationDomainGuard, key string) (model.ConfigurationResource, error) {
	var row model.ConfigurationResource
	if _, err := configurationDomain(ctx, tx, domain); err != nil {
		return row, err
	}
	if _, _, err := model.NormalizeConfigurationName(model.ConfigurationResourceName, key); err != nil || strings.TrimSpace(key) != key {
		return row, failure("invalid_argument", "invalid system resource key")
	}
	var archived bool
	var body []byte
	err := tx.QueryRowContext(ctx, configurationSystemResourceQuery, scope, domain.Key, key).Scan(&row.ID, &row.NamespaceID, &row.Name, &row.NameKey, &row.Revision, &row.MainCommitID, &archived, &row.OriginResourceID, &row.OriginCommitID, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return row, failure("not_found", "system resource not found")
	}
	if err != nil {
		return row, err
	}
	if len(body) > MaxResponseBytes {
		return row, failure("resource_exhausted", "system resource metadata exceeds response bound")
	}
	out, err := configurationDecodeResourceRow(row, archived, body)
	if err != nil {
		return model.ConfigurationResource{}, err
	}
	if !out.System || out.SystemKey != key {
		return model.ConfigurationResource{}, failure("data_loss", "system resource identity disagrees")
	}
	if archived {
		return model.ConfigurationResource{}, failure("not_found", "live system resource not found")
	}
	return out, nil
}
