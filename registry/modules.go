// Package registry is storage-owned static data-module composition.
package registry

import (
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
)

// Compiled lists the data modules this binary can install and migrate. A
// manifest namespace uses one by naming its identifier.
func Compiled() []engine.Module {
	return []engine.Module{coredata.Module()}
}
