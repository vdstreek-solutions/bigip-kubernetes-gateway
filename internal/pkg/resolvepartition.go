package pkg

// ResolvePartition returns the configured Partition override (BIGIPConfig.Partition, from the
// per-environment bigip-kubernetes-gateway-config) if one is set, otherwise fallback -- whatever
// this project would have used on its own (a K8s namespace for pools, a GatewayClass name for
// Virtuals/iRules). See BIGIPConfig.Partition's doc comment in types.go for why this needs to
// exist at all: those two defaults are independent and neither maps to "one partition per
// environment," which is what a route/self-IP/tunnel set up in a specific partition actually
// needs to be visible to.
func ResolvePartition(fallback string) string {
	for _, c := range BIPConfigs {
		if c.Partition != "" {
			return c.Partition
		}
	}
	return fallback
}

// ResolveBackendSnatAddress returns the configured BackendSnatAddress (see its doc comment in
// types.go) from the first BIGIPConfig entry that sets one, or "" if none do -- callers should
// fall back to SNAT automap in that case.
func ResolveBackendSnatAddress() string {
	for _, c := range BIPConfigs {
		if c.BackendSnatAddress != "" {
			return c.BackendSnatAddress
		}
	}
	return ""
}
