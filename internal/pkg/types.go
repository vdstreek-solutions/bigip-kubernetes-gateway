package pkg

import (
	"sync"

	v1 "k8s.io/api/core/v1"
	gatewayapi "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

type CtxKeyType string

type SIGCache struct {
	mutex          sync.RWMutex
	SyncedAtStart  bool
	ControllerName string
	Gateway        map[string]*gatewayapi.Gateway
	ListenerSet    map[string]*gatewayapi.ListenerSet
	HTTPRoute      map[string]*gatewayapi.HTTPRoute
	Endpoints      map[string]*v1.Endpoints
	Service        map[string]*v1.Service
	GatewayClass   map[string]*gatewayapi.GatewayClass
	Namespace      map[string]*v1.Namespace
	ReferenceGrant map[string]*gatewayv1beta1.ReferenceGrant
	Secret         map[string]*v1.Secret
}

type ReferenceGrantFromTo map[string]map[string]int8

type BIGIPConfigs []BIGIPConfig
type BIGIPConfig struct {
	Management struct {
		Username  string
		IpAddress string `yaml:"ipAddress"`
		Port      *int
		// PeerIpAddress is the OTHER unit's own management IP in an HA pair, for deployments
		// where ipAddress is a specific unit's non-floating management address rather than
		// something that follows HA state -- confirmed live for this cluster: the BIG-IP
		// management IP is pinned to one physical unit, so after a failover the controller kept
		// targeting what had become the standby unit. Optional and backward compatible: leaving
		// it unset (or empty) preserves the old fixed-single-address behavior exactly, with no
		// failover-status check at all. When set, AS3Deployer checks which of ipAddress/
		// PeerIpAddress currently reports itself as the active HA member before every deploy
		// (see ensureActiveBIGIPTarget in utils.go) and targets that one, rather than caching the
		// choice once and letting it go stale across a live failover.
		PeerIpAddress *string `yaml:"peerIpAddress"`
	}
	// Vtep is per-BIG-IP because the tunnel/partition it names lives on that specific device --
	// environments each have their own partition (e.g. K8S-D for dev), so this belongs in the
	// per-environment config file (overlays/<env>/configmap.yaml), not a CLI flag on the shared
	// base Deployment. Empty TunnelName disables the FDB sync for this BIG-IP entirely.
	Vtep struct {
		TunnelName      string `yaml:"tunnelName"`
		TunnelPartition string `yaml:"tunnelPartition"`
	}
	// AddressPoolStart/AddressPoolEnd: inclusive begin/end range to auto-assign Gateway VIPs
	// from when a Gateway's spec.addresses is empty -- see internal/pkg/addresspool.go. A plain
	// range rather than a CIDR, since network teams commonly hand out an arbitrary sub-range of
	// a VLAN that isn't itself CIDR-aligned. Environment-specific (dev/test/acceptance/
	// production each get their own VIP range), hence living here rather than a CLI flag on the
	// shared base Deployment. Only the first entry with BOTH fields set, across all BIGIPConfig
	// entries, is used (one shared pool, not per-device -- see addresspool.go's InitAddressPool
	// comment for why).
	AddressPoolStart string `yaml:"addressPoolStart"`
	AddressPoolEnd   string `yaml:"addressPoolEnd"`
	// Partition overrides the AS3 tenant (BIG-IP partition) that pools, Virtual Servers, and
	// iRules land in. Without it, this project defaults to two independent, unrelated naming
	// schemes: pools land in the backend Service's K8s namespace, while Virtuals/iRules land in
	// the GatewayClass's own name -- a "K8s namespace = tenant" model, not this org's actual
	// "one partition per environment" model. Setting Partition forces both onto the same value,
	// matching where Vtep.TunnelPartition's tunnel/route/self-IP already live -- otherwise BIG-IP
	// silently can't route to pools it can't see a route for. See internal/pkg/resolvepartition.go.
	// Only the first non-empty one across all BIGIPConfig entries is used, same reasoning as
	// AddressPoolStart/AddressPoolEnd above.
	Partition string `yaml:"partition"`
	// BackendSnatAddress, if set, is a single IP address (e.g. "10.21.0.1", the tunnel's own
	// self-IP) that Gateway Virtual Servers should SNAT backend-side connections from, instead of
	// SNAT automap. Automap lets BIG-IP pick any enabled self-IP for the backend-side connection,
	// including ones outside the VTEP tunnel's CIDR (e.g. a FrontendVIPs self-IP) -- Cilium's
	// vtep.cidr only accepts return traffic for source IPs inside the tunnel's own subnet, so an
	// automap-chosen address outside it makes BIG-IP's SYN to the pod go unanswered and the
	// connection gets reset after ~12s (confirmed via packet capture, not just theoretical). Use
	// the tunnel's own self-IP here (the same address Vtep-path health monitors already use
	// successfully).
	//
	// This is declared as an AS3-managed SNAT_Pool (see parser.go), not a {"bigip": "/path"}
	// pointer to a manually tmsh-created TMOS object -- that was tried first and the manually
	// created pool repeatedly vanished from BIG-IP between deploys (confirmed live, twice): AS3's
	// "selective" tenant sync doesn't reliably leave non-AS3-owned objects of this type alone.
	// Empty means "auto" (automap), the old, broken-for-this-topology default.
	BackendSnatAddress string `yaml:"backendSnatAddress"`
}
