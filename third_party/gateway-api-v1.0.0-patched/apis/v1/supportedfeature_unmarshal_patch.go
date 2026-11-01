package v1

import "encoding/json"

// UnmarshalJSON is a local patch, not upstream: v1.0.0's SupportedFeature is a bare string, but a
// cluster running newer Gateway API CRDs (this one: v1.6.1, see fleet-platform's
// rings/00-core/gateway-api-crds) serializes GatewayClassStatus.supportedFeatures as
// {"name": "..."} objects (the shape SupportedFeature became as of upstream v1.2.0). Without this,
// decoding any GatewayClass whose status was written by a newer-Gateway-API-aware controller (e.g.
// Cilium's own) fails outright, which breaks the informer for ALL GatewayClasses cluster-wide, not
// just the ones this controller cares about -- this project never reads or writes
// SupportedFeatures itself (grepped internal/ -- zero references), so tolerating both shapes on
// the way in is sufficient; no matching MarshalJSON needed.
func (s *SupportedFeature) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		*s = SupportedFeature(str)
		return nil
	}

	var obj struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return err
	}
	*s = SupportedFeature(obj.Name)
	return nil
}
