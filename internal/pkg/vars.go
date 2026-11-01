package pkg

import (
	"sync"

	f5_bigip "github.com/f5devcentral/f5-bigip-rest-go/bigip"
	"github.com/f5devcentral/f5-bigip-rest-go/utils"
)

var (
	PendingDeploys *utils.DeployQueue
	DoneDeploys    *utils.DeployQueue
	ActiveSIGs     *SIGCache
	BIGIPs         []*f5_bigip.BIGIP
	BIPConfigs     BIGIPConfigs
	BIPPassword    string
	refFromTo      *ReferenceGrantFromTo
	LogLevel       string

	// bigipMu guards every read or copy of a *f5_bigip.BIGIP in BIGIPs, not just .URL writes --
	// ensureActiveBIGIPTarget (utils.go) mutates bip.URL in place from AS3Deployer's goroutine on
	// every deploy (retargeting to the HA peer on failover), while SyncNodeFDBs (nodefdb.go) reads
	// the same *bip from NodeReconciler's own goroutine on every node add/update/delete. Dereferencing
	// *bip to copy it (BIGIPContext{BIGIP: *bip}) races with that write exactly like a direct field
	// read would, so every such dereference goes through snapshotBIGIP, and every .URL write through
	// setBIGIPURL, rather than touching bip/bip.URL directly.
	bigipMu sync.RWMutex
)

// snapshotBIGIP returns a race-free copy of *bip, for building a BIGIPContext without racing
// ensureActiveBIGIPTarget's concurrent writes to bip.URL. Use this instead of dereferencing *bip
// directly anywhere that can run concurrently with a deploy (i.e. outside AS3Deployer's own
// goroutine, which already serializes its own reads against its own writes).
func snapshotBIGIP(bip *f5_bigip.BIGIP) f5_bigip.BIGIP {
	bigipMu.RLock()
	defer bigipMu.RUnlock()
	return *bip
}

// setBIGIPURL writes bip.URL under the same lock snapshotBIGIP reads under.
func setBIGIPURL(bip *f5_bigip.BIGIP, url string) {
	bigipMu.Lock()
	defer bigipMu.Unlock()
	bip.URL = url
}

// getBIGIPURL reads bip.URL under the same lock setBIGIPURL writes under.
func getBIGIPURL(bip *f5_bigip.BIGIP) string {
	bigipMu.RLock()
	defer bigipMu.RUnlock()
	return bip.URL
}

// const (
// 	DeployMethod_AS3  = "as3"
// 	DeployMethod_REST = "rest"
// )
