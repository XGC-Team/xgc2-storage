package coredata

import (
	"context"
	"database/sql"
	"errors"
	"sort"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// ReadCatalogHeaderPage obtains the relevant catalog cut and its current-main
// headers in one reader transaction. Unrelated domain, selector, namespace,
// archive, and protection changes cannot invalidate the cut.
func (s *Store) ReadCatalogHeaderPage(ctx context.Context, q model.ConfigurationCatalogPageRead) (model.ConfigurationCatalogHeaderPage, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationCatalogHeaderPage, error) {
		var out model.ConfigurationCatalogHeaderPage
		if q.Catalog.ID != "" || q.ExpectedGeneration != nil && *q.ExpectedGeneration < 0 ||
			q.OrdinaryOnly && (q.Selector == "" || q.Catalog.IncludeArchived) || q.ChoiceOnly && !q.OrdinaryOnly ||
			q.Selector != "" && !textKey(q.Selector) {
			return out, failure("invalid_argument", "invalid catalog cut selector")
		}
		if _, err := configurationDomain(ctx, tx, q.Catalog.Domain); err != nil {
			return out, err
		}
		if _, _, _, err := configurationResourceCatalogSelector(scope, q.Catalog); err != nil {
			return out, err
		}
		generation, err := configurationCatalogGeneration(ctx, tx, scope, q)
		if err != nil {
			return out, err
		}
		if q.ExpectedGeneration != nil && generation != *q.ExpectedGeneration {
			return out, failure("conflict", "selected configuration catalog changed")
		}
		if q.ChoiceOnly {
			out.Choices, err = configurationChoicePageInDomain(ctx, tx, scope, q.Catalog, q.Selector, q.OrdinaryOnly)
		} else {
			out.Headers, err = configurationHeaderPageInDomain(ctx, tx, scope, q.Catalog, q.Selector, q.OrdinaryOnly)
		}
		out.Generation = generation
		return out, err
	})
}

func configurationCatalogGeneration(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationCatalogPageRead) (int64, error) {
	scoped, namespace := 0, ""
	if q.Catalog.ParentID != nil {
		scoped, namespace = 1, *q.Catalog.ParentID
	}
	column := "live_generation"
	if q.ChoiceOnly {
		column = "choice_generation"
	} else if q.OrdinaryOnly {
		column = "ordinary_generation"
	} else if q.Catalog.IncludeArchived {
		column = "all_generation"
	}
	var value int64
	err := tx.QueryRowContext(ctx, "SELECT "+column+" FROM core_configuration_catalog_cuts WHERE scope=? AND domain=? AND selector=? AND namespace_scoped=? AND namespace_id=?", scope, q.Catalog.Domain.Key, q.Selector, scoped, namespace).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return value, err
}

type catalogCutKey struct {
	selector, namespace string
	scoped              int
}

type catalogCutChange struct {
	live, ordinary            bool
	beforeChoice, afterChoice bool
}

// bumpCatalogCuts updates only the cuts containing either side of the actual
// resource change. These revisions are derived transactional invalidation
// metadata, not catalog content or a second ownership authority.
func bumpCatalogCuts(ctx context.Context, tx *sql.Tx, scope, domain string, before, after *model.ConfigurationResource) error {
	changes := map[catalogCutKey]catalogCutChange{}
	choiceFieldsChanged := before == nil || after == nil || before.ID != after.ID || before.Name != after.Name ||
		before.MainCommitID != after.MainCommitID || before.SystemKey != after.SystemKey
	add := func(resource *model.ConfigurationResource, isBefore bool) {
		if resource == nil {
			return
		}
		selectors := []string{""}
		if resource.CatalogSelector != "" {
			selectors = append(selectors, resource.CatalogSelector)
		}
		for _, selector := range selectors {
			for _, key := range []catalogCutKey{{selector: selector}, {selector: selector, scoped: 1, namespace: resource.NamespaceID}} {
				change := changes[key]
				live := resource.ArchivedAt == ""
				change.live = change.live || live
				change.ordinary = change.ordinary || live && !resource.System
				if isBefore {
					change.beforeChoice = live && !resource.System
				} else {
					change.afterChoice = live && !resource.System
				}
				changes[key] = change
			}
		}
	}
	add(before, true)
	add(after, false)
	keys := make([]catalogCutKey, 0, len(changes))
	for key := range changes {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].selector != keys[j].selector {
			return keys[i].selector < keys[j].selector
		}
		if keys[i].scoped != keys[j].scoped {
			return keys[i].scoped < keys[j].scoped
		}
		return keys[i].namespace < keys[j].namespace
	})
	const upsert = `INSERT INTO core_configuration_catalog_cuts(scope,domain,selector,namespace_scoped,namespace_id,all_generation,live_generation,ordinary_generation,choice_generation)
		VALUES(?,?,?,?,?,1,?,?,?) ON CONFLICT(scope,domain,selector,namespace_scoped,namespace_id) DO UPDATE SET
		all_generation=all_generation+1,live_generation=live_generation+excluded.live_generation,ordinary_generation=ordinary_generation+excluded.ordinary_generation,choice_generation=choice_generation+excluded.choice_generation
		WHERE all_generation<9223372036854775807 AND live_generation<=9223372036854775807-excluded.live_generation
		AND ordinary_generation<=9223372036854775807-excluded.ordinary_generation AND choice_generation<=9223372036854775807-excluded.choice_generation RETURNING all_generation`
	var addedRows, addedBytes int64
	for _, key := range keys {
		change := changes[key]
		choiceChanged := change.beforeChoice != change.afterChoice || change.beforeChoice && choiceFieldsChanged
		var generation int64
		err := tx.QueryRowContext(ctx, upsert, scope, domain, key.selector, key.scoped, key.namespace, change.live, change.ordinary, choiceChanged).Scan(&generation)
		if errors.Is(err, sql.ErrNoRows) {
			return failure("resource_exhausted", "configuration catalog generation exhausted")
		}
		if err != nil {
			return err
		}
		if generation == 1 {
			addedRows++
			addedBytes += int64(len(scope) + len(domain) + len(key.selector) + len(key.namespace) + 64)
		}
	}
	if addedRows != 0 {
		return reserve(ctx, tx, scope, addedRows, addedBytes)
	}
	return nil
}
