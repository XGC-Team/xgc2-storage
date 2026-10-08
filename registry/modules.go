// Package registry is storage-owned static data-module composition.
package registry

import (
	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
)

func Compiled() []engine.DataModule {
	return []engine.DataModule{{Spec: coredata.Spec(), Initialize: coredata.Initialize, Execute: coredata.Execute}}
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
