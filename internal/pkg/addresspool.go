package pkg

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"

	"github.com/f5devcentral/f5-bigip-rest-go/utils"
)

type addressPool struct {
	mu        sync.Mutex
	start     net.IP
	end       net.IP
	allocated map[string]string // gateway key ("namespace/name") -> assigned IP
	used      map[string]bool   // assigned IP -> true, for O(1) collision checks
}

var pool *addressPool

// InitAddressPool reads AddressPoolStart/AddressPoolEnd from the first BIGIPConfig entry that
// sets both (BIPConfigs, from the per-environment bigip-kubernetes-gateway-config --
// overlays/<env>/configmap.yaml in fleet-platform). A plain begin/end range rather than a CIDR:
// network teams commonly hand out an arbitrary sub-range of a VLAN that isn't itself
// CIDR-aligned (e.g. "use .10 through .25 of this /24"), which a CIDR can't express without
// forcing a power-of-2-sized, boundary-aligned block. Every address in [start, end] is
// considered usable -- unlike CIDR mode, there's no network/broadcast address to exclude, since
// the caller is expected to have already given a range of genuinely assignable addresses.
//
// One shared pool across all configured BIG-IPs, not per-device like Vtep: a Gateway's Virtual
// Server gets deployed identically to every configured BIG-IP (AS3Deployer), so a single shared
// VIP per Gateway is the correct model here. Call once at startup, after setupBIGIPs populates
// BIPConfigs; no-op if neither is set.
func InitAddressPool() error {
	start, end := "", ""
	for _, c := range BIPConfigs {
		if c.AddressPoolStart != "" && c.AddressPoolEnd != "" {
			start, end = c.AddressPoolStart, c.AddressPoolEnd
			break
		}
	}
	if start == "" || end == "" {
		return nil
	}

	startIP := net.ParseIP(start).To4()
	if startIP == nil {
		return fmt.Errorf("invalid addressPoolStart %q: not a valid IPv4 address", start)
	}
	endIP := net.ParseIP(end).To4()
	if endIP == nil {
		return fmt.Errorf("invalid addressPoolEnd %q: not a valid IPv4 address", end)
	}
	if binary.BigEndian.Uint32(startIP) > binary.BigEndian.Uint32(endIP) {
		return fmt.Errorf("addressPoolStart %q is after addressPoolEnd %q", start, end)
	}

	pool = &addressPool{
		start:     startIP,
		end:       endIP,
		allocated: map[string]string{},
		used:      map[string]bool{},
	}
	return nil
}

// AddressPoolConfigured reports whether InitAddressPool found a range to use.
func AddressPoolConfigured() bool {
	return pool != nil
}

// RegisterExistingAddress marks ip as already assigned to gatewayKey, without
// allocating a new one. Idempotent -- called on every reconcile of a Gateway
// that already has a status.addresses entry, so a controller restart re-derives
// the same allocation state from the cluster's own Gateway objects rather than
// needing persisted state of its own.
//
// Returns false (and does NOT register) if ip falls outside the currently
// configured [start, end] -- e.g. addressPoolStart/addressPoolEnd changed since
// this Gateway was allocated. The caller should treat false as "no existing
// allocation" and fall through to AllocateAddress, rather than this address
// getting permanently stuck outside a range that moved out from under it
// (confirmed happening in practice, not just a theoretical edge case).
func RegisterExistingAddress(gatewayKey, ip string) bool {
	if pool == nil {
		return false
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()

	parsed := net.ParseIP(ip).To4()
	if parsed == nil || !inRange(parsed) {
		return false
	}

	if existing, ok := pool.allocated[gatewayKey]; ok && existing == ip {
		return true
	}

	// pool.used[ip] can only be true here because some OTHER gatewayKey already claimed it -- the
	// check just above already handled "this gatewayKey re-registering its own existing ip".
	// Overwriting pool.allocated[gatewayKey] in that case would silently steal the address (e.g.
	// two Gateways ending up with the same IP in their own status.addresses, from a stale status
	// or a copy-pasted manifest), and AS3 then rejects the WHOLE tenant -- not just these two
	// Gateways -- the moment both Virtual Servers get declared at the same destination IP.
	// Refuse instead: same "no existing allocation" contract as the out-of-range case above, so
	// the caller falls through to AllocateAddress for a genuinely free address.
	if pool.used[ip] {
		owner := "unknown gateway"
		for k, v := range pool.allocated {
			if v == ip {
				owner = k
				break
			}
		}
		utils.LogFromContext(context.TODO()).Errorf(
			"address pool conflict: %s already owns %s; refusing to let %s claim the same address "+
				"from its Gateway status -- it will get a fresh one from AllocateAddress instead",
			owner, ip, gatewayKey)
		return false
	}

	pool.allocated[gatewayKey] = ip
	pool.used[ip] = true
	return true
}

// inRange reports whether ip falls within [pool.start, pool.end]. Caller must hold pool.mu.
func inRange(ip net.IP) bool {
	n := binary.BigEndian.Uint32(ip)
	return n >= binary.BigEndian.Uint32(pool.start) && n <= binary.BigEndian.Uint32(pool.end)
}

// AllocateAddress returns gatewayKey's existing allocation if one exists,
// otherwise assigns and returns the next free IP in [AddressPoolStart, AddressPoolEnd].
// Returns an error if the pool is exhausted or not configured.
func AllocateAddress(gatewayKey string) (string, error) {
	if pool == nil {
		return "", fmt.Errorf("addressPoolStart/addressPoolEnd are not configured")
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()

	if existing, ok := pool.allocated[gatewayKey]; ok {
		return existing, nil
	}

	end := binary.BigEndian.Uint32(pool.end)
	for ip := pool.start; binary.BigEndian.Uint32(ip) <= end; ip = nextAddr(ip) {
		s := ip.String()
		if !pool.used[s] {
			pool.allocated[gatewayKey] = s
			pool.used[s] = true
			return s, nil
		}
	}
	return "", fmt.Errorf("address pool %s-%s is exhausted", pool.start, pool.end)
}

func nextAddr(ip net.IP) net.IP {
	v4 := ip.To4()
	n := binary.BigEndian.Uint32(v4)
	out := make(net.IP, 4)
	binary.BigEndian.PutUint32(out, n+1)
	return out
}
