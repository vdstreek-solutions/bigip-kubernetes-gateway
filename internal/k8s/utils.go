package k8s

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/f5devcentral/f5-bigip-rest-go/utils"
	v1 "k8s.io/api/core/v1"
)

// wantPortNames, when non-empty, restricts the returned members to the Service's own port(s)
// whose Name is in the set -- see FormatMembersFromServiceEndpoints's doc comment for why this
// matters. An empty/nil set means "unknown, include every port" (the prior, unfiltered behavior),
// used when nothing tells us which of the Service's ports is actually wanted.
func FormatMembersFromServiceEndpoints(svc *v1.Service, eps *v1.Endpoints, wantPortNames map[string]bool) ([]SvcEpsMember, error) {
	if eps == nil || svc == nil {
		return []SvcEpsMember{}, fmt.Errorf("the given service or endpoints is nil")
	}

	members := []SvcEpsMember{}
	serviceType := svc.Spec.Type

	switch serviceType {
	case v1.ServiceTypeNodePort: // "NodePort"
		nodeIPs := []string{}
		for _, nd := range NodeCache.All() {
			if nd.IpAddr == "" {
				return []SvcEpsMember{}, utils.RetryErrorf("node ip %s not found yet", nd.Name)
			}
			nodeIPs = append(nodeIPs, nd.IpAddr)
		}

		for _, port := range svc.Spec.Ports {
			// A Service commonly exposes ports for unrelated purposes -- e.g. one for real
			// traffic, another purely for a telemetry/health endpoint -- and without this filter
			// every port's members were pooled together as if interchangeable. Confirmed live:
			// dex's Service exposes "http" (5556, the OIDC endpoint an HTTPRoute actually routes
			// to) and "telemetry" (5558, k8s-probe-only), both landed in the same AS3 pool, and
			// requests randomly load-balanced onto whichever port happened to be picked -- roughly
			// half returned 404 because port 5558 doesn't serve any of the OIDC/dex HTTP routes.
			if len(wantPortNames) > 0 && !wantPortNames[port.Name] {
				continue
			}
			for _, ip := range nodeIPs {
				members = append(members, SvcEpsMember{
					// TargetPort: port.TargetPort.IntValue(),
					// NodePort:   int(port.NodePort),
					TargetPort: int(port.NodePort),
					IpAddr:     ip,
				})
			}
		}
	case v1.ServiceTypeClusterIP: // "ClusterIP"
		for _, subset := range eps.Subsets {
			for _, port := range subset.Ports {
				// See the matching comment in the NodePort case above -- same filter, applied via
				// the Endpoints subset port's Name (which the endpoint controller sets to match
				// the originating Service port's Name for a multi-port Service).
				if len(wantPortNames) > 0 && !wantPortNames[port.Name] {
					continue
				}
				for _, addr := range subset.Addresses {
					member := SvcEpsMember{
						TargetPort: int(port.Port),
						IpAddr:     addr.IP,
					}
					if addr.NodeName == nil {
						return []SvcEpsMember{}, fmt.Errorf("%s node name was not appointed in endpoints", addr.IP)
					}
					if k8no := NodeCache.Get(*addr.NodeName); k8no == nil {
						return []SvcEpsMember{}, utils.RetryErrorf("%s not found yet", *addr.NodeName)
					} else if k8no.NetType == "vxlan" {
						if utils.IsIpv6(addr.IP) {
							member.MacAddr = k8no.MacAddrV6
						} else {
							member.MacAddr = k8no.MacAddr
						}
					}
					members = append(members, member)
				}
			}
		}
	case v1.ServiceTypeLoadBalancer: // "LoadBalancer"
		return []SvcEpsMember{}, fmt.Errorf("not supported service type: %s", serviceType)
	case v1.ServiceTypeExternalName: // "ExternalName"
		return []SvcEpsMember{}, fmt.Errorf("not supported service type: %s", serviceType)
	default:
		return []SvcEpsMember{}, fmt.Errorf("unknown service type: %s", serviceType)
	}

	// Dedupe by (IpAddr, TargetPort): a Service with multiple named ports that all target the
	// same container port (e.g. argocd-server's 80 and 443 both -> 8080) produces an Endpoints
	// subset with multiple Port entries sharing that same number, and the ClusterIP loop above
	// has no reason to know that -- it just appends one member per (port, address) pair. AS3
	// then rejects the whole tenant's declaration with "declaration has duplicate values in
	// members" -- confirmed live, and since it's a tenant-wide rejection it broke every other
	// pool/Virtual Server in the partition too, not just this Service's own pool.
	seen := map[string]bool{}
	deduped := []SvcEpsMember{}
	for _, m := range members {
		key := fmt.Sprintf("%s:%d", m.IpAddr, m.TargetPort)
		if seen[key] {
			continue
		}
		seen[key] = true
		deduped = append(deduped, m)
	}

	return deduped, nil
}

func detectCNIType(node *v1.Node) string {
	kind := "unknown"

	for _, c := range node.Status.Conditions {
		if c.Reason == "CiliumIsUp" {
			kind = "cilium"
			break
		}
		if c.Reason == "CalicoIsUp" {
			kind = "calico"
			break
		}
		if c.Reason == "FlannelIsUp" {
			kind = "flannel"
			break
		}
	}
	return kind
}

// Convert an IPV4 string to a fake MAC address.
func ipv4ToMac(addr string) string {
	return Ipv4ToMac(addr)
}

// Ipv4ToMac is the exported form of ipv4ToMac, for the VTEP FDB sync in
// internal/pkg (fake per-node MAC, same 0a:0a:xx:xx:xx:xx scheme F5's own
// CIS controller uses for Flannel/OpenShift-SDN VXLAN integration).
func Ipv4ToMac(addr string) string {
	ip := strings.Split(addr, ".")
	if len(ip) != 4 {
		return ""
	}
	var intIP [4]int
	for i, val := range ip {
		intIP[i], _ = strconv.Atoi(val)
	}
	return fmt.Sprintf("0a:0a:%02x:%02x:%02x:%02x", intIP[0], intIP[1], intIP[2], intIP[3])
}
