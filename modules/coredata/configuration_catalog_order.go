package coredata

import "github.com/XGC-Team/xgc2-storage/modules/coredata/model"

func catalogPageOrdering(order model.ConfigurationCatalogOrder) string {
	const names = "r.name_key, r.id"
	// Core writes UTC RFC3339Nano. Removing Z makes whole seconds sort
	// before fractional values within the same second.
	const updated = "rtrim(json_extract(CAST(r.body AS TEXT),'$.updated_at'),'Z')"
	switch order {
	case model.ConfigurationCatalogNameOrder:
		return names
	case model.ConfigurationCatalogOldestOrder:
		return updated + " ASC, " + names
	case model.ConfigurationCatalogNewestOrder:
		return updated + " DESC, " + names
	default:
		return ""
	}
}
