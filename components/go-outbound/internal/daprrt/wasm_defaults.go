package daprrt

import (
	_ "unsafe" // go:linkname

	"github.com/dapr/dapr/pkg/config"
)

// daprDefaultFeatures is Dapr's table of features enabled unless a
// Configuration says otherwise.
//
//go:linkname daprDefaultFeatures github.com/dapr/dapr/pkg/config.defaultFeatures
var daprDefaultFeatures map[config.Feature]bool

func init() {
	// HotReload watches the resources directory with fsnotify, and WASI has
	// no file-watch API: starting the watcher fails the whole runtime. Turn
	// the default off; a Configuration that enables it explicitly still fails.
	daprDefaultFeatures[config.HotReload] = false
}
