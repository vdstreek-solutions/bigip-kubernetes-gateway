package pkg

import (
	"context"
	"fmt"

	"github.com/f5devcentral/bigip-kubernetes-gateway/internal/k8s"
	f5_bigip "github.com/f5devcentral/f5-bigip-rest-go/bigip"
	"github.com/f5devcentral/f5-bigip-rest-go/utils"
)

// SyncNodeFDBs pushes a fake FDB record per known Kubernetes node
// (0a:0a:<node-ip-octets-in-hex> -> node's real IP) to each configured BIG-IP's
// own Vtep.TunnelName tunnel (BIGIPConfig, from the per-environment
// bigip-kubernetes-gateway-config -- overlays/<env>/configmap.yaml in
// fleet-platform), mirroring what F5's own CIS controller automates for
// Flannel/OpenShift-SDN via --openshift-sdn-name/--flannel-name (see
// f5devcentral/f5-ci-docs, docs/cilium/cilium-bigip-info.rst) -- this project
// has no built-in Cilium VTEP awareness, hence this file. A BIG-IP with an
// empty Vtep.TunnelName is skipped entirely (feature off for that device).
//
// Recomputes and pushes the FULL desired record set on every call (an
// idempotent state sync, not a diff against BIG-IP's existing records) --
// simpler and self-healing versus tracking per-node add/remove deltas, and
// cheap at Kubernetes node-cluster scale. Called from NodeReconciler on
// every node add/update/delete.
func SyncNodeFDBs(ctx context.Context) error {
	slog := utils.LogFromContext(ctx)

	records := []map[string]interface{}{}
	for _, node := range k8s.NodeCache.All() {
		if node.IpAddr == "" {
			continue
		}
		records = append(records, map[string]interface{}{
			"name":     k8s.Ipv4ToMac(node.IpAddr),
			"endpoint": node.IpAddr,
		})
	}
	body := map[string]interface{}{"records": records}

	errs := []error{}
	// BIGIPs and BIPConfigs are index-aligned -- setupBIGIPs (cmd/controller/main.go) builds
	// BIGIPs by appending in the same order it iterates BIPConfigs.
	for i, bip := range BIGIPs {
		if i >= len(BIPConfigs) || BIPConfigs[i].Vtep.TunnelName == "" {
			continue
		}
		vtep := BIPConfigs[i].Vtep
		bc := &f5_bigip.BIGIPContext{Context: ctx, BIGIP: snapshotBIGIP(bip)}
		if err := bc.Update("net/fdb/tunnel", vtep.TunnelName, vtep.TunnelPartition, "", body); err != nil {
			slog.Errorf("failed to sync VTEP FDB records to %s (%s/%s): %s", getBIGIPURL(bip), vtep.TunnelPartition, vtep.TunnelName, err.Error())
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return fmt.Errorf("failed to sync VTEP FDB records to %d BIG-IP(s)", len(errs))
	}
	return nil
}
