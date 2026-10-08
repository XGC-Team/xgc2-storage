package coredata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// DeclareConfigurationDomains is an explicit owner-held deployment transaction
// seam. No Named request calls it, no schema/adoption fallback exists, and this
// function opens no connection. Existing fixed capabilities cannot be changed.
func DeclareConfigurationDomains(ctx context.Context, tx *sql.Tx, domains []model.ConfigurationDomainDeclaration) error {
	if tx == nil || len(domains) == 0 || len(domains) > 256 {
		return failure("invalid_argument", "bounded explicit domain declarations required")
	}
	return atomicData(ctx, tx, func() error {
		seen := map[string]bool{}
		for _, d := range domains {
			if !textKey(d.Key) || !textKey(d.SchemaIdentity) || d.SchemaVersion < 1 || !canonicalSessionPin(d.RegistryDigest) || len(d.Capabilities) > 64 || seen[d.Key] {
				return failure("invalid_argument", "invalid domain declaration")
			}
			seen[d.Key] = true
			caps := append([]string{}, d.Capabilities...)
			sort.Strings(caps)
			for i, c := range caps {
				if !textKey(c) || i > 0 && c == caps[i-1] {
					return failure("invalid_argument", "invalid domain capabilities")
				}
			}
			body, err := encode(caps)
			if err != nil {
				return err
			}
			var version int
			var old []byte
			var visibility, system bool
			err = tx.QueryRowContext(ctx, "SELECT schema_version,capabilities,main_visibility,system_provisioning FROM core_configuration_domains WHERE domain=?", d.Key).Scan(&version, &old, &visibility, &system)
			if err == nil {
				if version != d.SchemaVersion || !bytes.Equal(old, body) || visibility != d.MainVisibility || system != d.AllowSystemProvisioning {
					return failure("conflict", "fixed domain schema or capabilities changed")
				}
				_, err = tx.ExecContext(ctx, "UPDATE core_configuration_domains SET schema_identity=?,registry_digest=? WHERE domain=?", d.SchemaIdentity, d.RegistryDigest, d.Key)
			} else if errors.Is(err, sql.ErrNoRows) {
				_, err = tx.ExecContext(ctx, "INSERT INTO core_configuration_domains VALUES(?,?,?,?,?,?,?)", d.Key, d.SchemaIdentity, d.SchemaVersion, d.RegistryDigest, body, d.MainVisibility, d.AllowSystemProvisioning)
			}
			if err != nil {
				return err
			}
		}
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM core_configuration_domains").Scan(&count); err != nil {
			return err
		}
		if count > 256 {
			return failure("resource_exhausted", "domain declaration count exceeded")
		}
		return nil
	})
}

func configurationDomain(ctx context.Context, tx *sql.Tx, d model.ConfigurationDomainGuard) (model.ConfigurationDomainDeclaration, error) {
	out := model.ConfigurationDomainDeclaration{Key: d.Key}
	if !textKey(d.Key) || !textKey(d.SchemaIdentity) || d.SchemaVersion < 1 || !canonicalSessionPin(d.RegistryDigest) {
		return out, failure("invalid_argument", "invalid domain guard")
	}
	var caps []byte
	err := tx.QueryRowContext(ctx, "SELECT schema_identity,schema_version,registry_digest,capabilities,main_visibility,system_provisioning FROM core_configuration_domains WHERE domain=?", d.Key).Scan(&out.SchemaIdentity, &out.SchemaVersion, &out.RegistryDigest, &caps, &out.MainVisibility, &out.AllowSystemProvisioning)
	if errors.Is(err, sql.ErrNoRows) {
		return out, failure("failed_precondition", "domain is not explicitly declared")
	}
	if err != nil {
		return out, err
	}
	if out.SchemaIdentity != d.SchemaIdentity || out.SchemaVersion != d.SchemaVersion || out.RegistryDigest != d.RegistryDigest {
		return out, failure("conflict", "accepted domain catalog changed")
	}
	if json.Unmarshal(caps, &out.Capabilities) != nil {
		return out, failure("data_loss", "invalid deployment capabilities")
	}
	return out, nil
}
