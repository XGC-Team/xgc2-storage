// Package registry is storage-owned static data-module composition.
package registry

import (
	"context"
	"database/sql"
	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func Compiled() []engine.DataModule {
	return []engine.DataModule{{Spec: coredata.Spec(), Initialize: coredata.Initialize, Execute: coredata.Execute}}
}

// ConfigurationDeployment retains the sole compiled catalog and supplies the
// configuration owner's explicit facts to its existing deployment transaction.
func ConfigurationDeployment(domains []model.ConfigurationDomainDeclaration) []engine.DataModule {
	modules := Compiled()
	modules[0].Deploy = func(ctx context.Context, tx *sql.Tx) error {
		return coredata.DeclareConfigurationDomains(ctx, tx, domains)
	}
	return modules
}
func TransportBounds(m api.Manifest) (request, response int) {
	request, response = api.MaxRequestBytes, api.MaxResponseBytes
	for _, n := range m.Namespaces {
		for _, module := range n.Modules {
			for _, o := range module.Operations {
				if o.MaxRequestBytes > request {
					request = o.MaxRequestBytes
				}
				if o.MaxResponseBytes > response {
					response = o.MaxResponseBytes
				}
			}
		}
	}
	return
}
