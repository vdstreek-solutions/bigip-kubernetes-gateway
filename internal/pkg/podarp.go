package pkg

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/f5devcentral/bigip-kubernetes-gateway/internal/k8s"
	f5_bigip "github.com/f5devcentral/f5-bigip-rest-go/bigip"
	"github.com/f5devcentral/f5-bigip-rest-go/utils"
	v1 "k8s.io/api/core/v1"
	gatewayapi "sigs.k8s.io/gateway-api/apis/v1"
)

// Static ARP for pool members, the other half of the Cilium VTEP setup next to SyncNodeFDBs.
//
// The FDB maps the fake per-node MAC 0a:0a:<node-ip-hex> to that node's IP, but nothing told
// BIG-IP to use that MAC for a pod: it ARPed for the pod over the tunnel and got the pod's own
// MAC back. That MAC is in no FDB record, so BIG-IP flooded every packet for the pod to every
// node. Confirmed live on K8S-D: each SYN reached the pod once per node, the pod answered each
// copy with a different ISN, BIG-IP reset the "unknown" SYN-ACKs (the RST flooded to all nodes
// as well), and both health checks and client connections were reset at random. F5 CIS pairs
// the same fake-MAC FDB with a static ARP entry per pod (pod IP -> 0a:0a:<hex of its node's IP>),
// which is what this keeps in place, so BIG-IP sends each packet to the one node hosting the pod.
//
// Entries live in each BIG-IP's Vtep.TunnelPartition, named podARPPrefix+<pod IP>; only entries
// with that prefix are ever changed or removed. Like SyncNodeFDBs it computes the full desired set
// every time and fixes up the difference, so a missed event or a manual delete heals on the next
// pass.
const (
	podARPPrefix = "k8s-pod-"
	// How long to wait after a change for more to come in. Endpoints churn arrives in bursts
	// (a rollout touches every replica), and each pass reads the full ARP list from BIG-IP.
	podARPDebounce = 2 * time.Second
	// Resync this often even without events, to heal anything changed on BIG-IP by hand.
	podARPResync = 5 * time.Minute
)

var podARPKick = make(chan struct{}, 1)

// RequestPodARPSync schedules a sync without blocking; requests made while one is pending are
// folded into it.
func RequestPodARPSync() {
	select {
	case podARPKick <- struct{}{}:
	default:
	}
}

// PodARPSyncer runs the sync loop. It does nothing until the caches are filled at startup:
// syncing against half-filled caches would delete the entries of every pod not cached yet.
func PodARPSyncer(stopCh chan struct{}) {
	if !vtepConfigured() {
		return
	}
	for !ActiveSIGs.SyncedAtStart {
		<-time.After(100 * time.Millisecond)
	}
	RequestPodARPSync()
	resync := time.NewTicker(podARPResync)
	defer resync.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-podARPKick:
			<-time.After(podARPDebounce)
			select {
			case <-podARPKick:
			default:
			}
		case <-resync.C:
		}
		ctx := NewContext()
		if err := SyncPodARPs(ctx); err != nil {
			utils.LogFromContext(ctx).Errorf(err.Error())
		}
	}
}

func vtepConfigured() bool {
	for _, c := range BIPConfigs {
		if c.Vtep.TunnelName != "" {
			return true
		}
	}
	return false
}

// desiredPodARPs maps every pod IP that is a member of one of our pools to the FDB MAC of the
// node it runs on. Pods whose node isn't cached yet are left out until a later pass. Pods on the
// host network share their node's IP, which BIG-IP reaches without the tunnel, so they're skipped
// too: an ARP entry for a node IP would pull the node itself into the tunnel.
func desiredPodARPs() map[string]string {
	nodeIPs := map[string]bool{}
	for _, n := range k8s.NodeCache.All() {
		nodeIPs[n.IpAddr] = true
	}
	arps := map[string]string{}
	for _, gwc := range ActiveSIGs.AllGatewayClasses() {
		if gwc.Spec.ControllerName != gatewayapi.GatewayController(ActiveSIGs.ControllerName) {
			continue
		}
		for _, svc := range ActiveSIGs.RelatedServices(gwc) {
			if svc.Spec.Type != v1.ServiceTypeClusterIP {
				continue
			}
			eps := ActiveSIGs.GetEndpoints(utils.Keyname(svc.Namespace, svc.Name))
			if eps == nil {
				continue
			}
			for _, subset := range eps.Subsets {
				for _, addr := range subset.Addresses {
					if addr.NodeName == nil || utils.IsIpv6(addr.IP) || nodeIPs[addr.IP] {
						continue
					}
					node := k8s.NodeCache.Get(*addr.NodeName)
					if node == nil || node.IpAddr == "" {
						continue
					}
					arps[addr.IP] = k8s.Ipv4ToMac(node.IpAddr)
				}
			}
		}
	}
	return arps
}

type podARPChanges struct {
	create, update map[string]string // pod IP -> MAC
	remove         []string          // entry names
}

func (c podARPChanges) empty() bool {
	return len(c.create) == 0 && len(c.update) == 0 && len(c.remove) == 0
}

// diffPodARPs compares the desired entries with the ones on BIG-IP (entry name -> [IP, MAC]),
// considering only entries this controller owns.
func diffPodARPs(desired map[string]string, existing map[string][2]string) podARPChanges {
	c := podARPChanges{create: map[string]string{}, update: map[string]string{}}
	have := map[string]string{}
	for name, e := range existing {
		if !strings.HasPrefix(name, podARPPrefix) {
			continue
		}
		ip, mac := e[0], e[1]
		want, ok := desired[ip]
		if !ok || name != podARPPrefix+ip {
			c.remove = append(c.remove, name)
			continue
		}
		have[ip] = mac
		if !strings.EqualFold(mac, want) {
			c.update[ip] = want
		}
	}
	for ip, mac := range desired {
		if _, ok := have[ip]; !ok {
			c.create[ip] = mac
		}
	}
	sort.Strings(c.remove)
	return c
}

// SyncPodARPs brings the static ARP entries on every BIG-IP with a VTEP tunnel in line with
// desiredPodARPs.
func SyncPodARPs(ctx context.Context) error {
	slog := utils.LogFromContext(ctx)
	desired := desiredPodARPs()

	errs := []error{}
	// BIGIPs and BIPConfigs are index-aligned, see SyncNodeFDBs.
	for i, bip := range BIGIPs {
		if i >= len(BIPConfigs) || BIPConfigs[i].Vtep.TunnelName == "" {
			continue
		}
		partition := BIPConfigs[i].Vtep.TunnelPartition
		bc := &f5_bigip.BIGIPContext{Context: ctx, BIGIP: snapshotBIGIP(bip)}
		existing, err := listPodARPs(bc, partition)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to list static ARP entries in %s on %s: %s", partition, getBIGIPURL(bip), err))
			continue
		}
		changes := diffPodARPs(desired, existing)
		if changes.empty() {
			continue
		}
		slog.Infof("pod ARP sync %s on %s: %d to create, %d to update, %d to remove",
			partition, getBIGIPURL(bip), len(changes.create), len(changes.update), len(changes.remove))
		for ip, mac := range changes.create {
			body := map[string]interface{}{"ipAddress": ip, "macAddress": mac}
			if err := bc.Deploy("net/arp", podARPPrefix+ip, partition, "", body); err != nil {
				errs = append(errs, fmt.Errorf("failed to create ARP %s -> %s in %s: %s", ip, mac, partition, err))
			}
		}
		for ip, mac := range changes.update {
			body := map[string]interface{}{"macAddress": mac}
			if err := bc.Update("net/arp", podARPPrefix+ip, partition, "", body); err != nil {
				errs = append(errs, fmt.Errorf("failed to update ARP %s -> %s in %s: %s", ip, mac, partition, err))
			}
		}
		for _, name := range changes.remove {
			if err := bc.Delete("net/arp", name, partition, ""); err != nil {
				errs = append(errs, fmt.Errorf("failed to remove ARP %s in %s: %s", name, partition, err))
			}
		}
	}
	for _, err := range errs {
		slog.Errorf(err.Error())
	}
	if len(errs) != 0 {
		return fmt.Errorf("pod ARP sync: %d operation(s) failed", len(errs))
	}
	return nil
}

// listPodARPs returns the static ARP entries in partition as entry name -> [IP, MAC].
func listPodARPs(bc *f5_bigip.BIGIPContext, partition string) (map[string][2]string, error) {
	resp, err := bc.All("net/arp?$filter=partition%20eq%20" + partition)
	if err != nil {
		return nil, err
	}
	rlt := map[string][2]string{}
	items, _ := (*resp)["items"].([]interface{})
	for _, item := range items {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		// Checked again rather than trusting the filter: the BIG-IP is shared, and another
		// cluster's controller keeps the same prefix in its own partition.
		if p, _ := m["partition"].(string); p != partition {
			continue
		}
		name, _ := m["name"].(string)
		ip, _ := m["ipAddress"].(string)
		mac, _ := m["macAddress"].(string)
		// A partition with a non-default route domain reports "10.160.5.247%2".
		ip, _, _ = strings.Cut(ip, "%")
		rlt[name] = [2]string{ip, mac}
	}
	return rlt, nil
}
