/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Hand-added to this locally vendored v1.0.0 copy of gateway-api, not upgraded wholesale: this
// controller's whole codebase (parser.go, cache.go, etc.) is written against the v1.0.0 type
// shapes, and a full version bump risks breaking changes across many call sites this session
// doesn't have the budget to re-verify one by one. ListenerSet is additive -- a namespaced
// resource that contributes extra listeners to an existing Gateway (see the live-installed CRD,
// gateway.networking.k8s.io bundle v1.6.1, `kubectl get crd listenersets.gateway.networking.k8s.io
// -o yaml` for the authoritative schema this was transcribed from) -- so it can be vendored on
// its own without touching anything else in this package. Field-for-field, a ListenerSet's own
// listener entries are identical to Gateway's existing Listener type (name, hostname, port,
// protocol, tls, allowedRoutes), so this reuses Listener directly rather than duplicating it.

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:resource:categories=gateway-api,shortName=lset
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Accepted",type=string,JSONPath=`.status.conditions[?(@.type=="Accepted")].status`
// +kubebuilder:printcolumn:name="Programmed",type=string,JSONPath=`.status.conditions[?(@.type=="Programmed")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ListenerSet defines a set of additional listeners to attach to an existing Gateway. This
// resource provides a mechanism to merge multiple listeners into a single Gateway.
//
// The parent Gateway must explicitly allow ListenerSet attachment through its AllowedListeners
// configuration. By default, Gateways do not allow ListenerSet attachment.
//
// Routes can attach to a ListenerSet by specifying it as a parentRef, and can optionally target
// specific listeners using the sectionName field.
type ListenerSet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec defines the desired state of ListenerSet.
	Spec ListenerSetSpec `json:"spec"`

	// Status defines the current state of ListenerSet.
	//
	// +kubebuilder:default={conditions: {{type: "Accepted", status: "Unknown", reason:"Pending", message:"Waiting for controller", lastTransitionTime: "1970-01-01T00:00:00Z"},{type: "Programmed", status: "Unknown", reason:"Pending", message:"Waiting for controller", lastTransitionTime: "1970-01-01T00:00:00Z"}}}
	Status ListenerSetStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ListenerSetList contains a list of ListenerSets.
type ListenerSetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ListenerSet `json:"items"`
}

// ListenerSetSpec defines the desired state of a ListenerSet.
type ListenerSetSpec struct {
	// ParentRef references the Gateway that the listeners are attached to.
	ParentRef ParentGatewayReference `json:"parentRef"`

	// Listeners associated with this ListenerSet. Field-for-field identical to Gateway's own
	// Listener type -- see this file's own doc comment for why it's reused directly instead of
	// being redefined. Each listener's Name only needs to be unique within this ListenerSet, not
	// across the whole merged Gateway+ListenerSet set (routes disambiguate by combining this
	// ListenerSet's identity with the listener's own name via sectionName).
	//
	// +kubebuilder:validation:MaxItems=64
	Listeners []Listener `json:"listeners"`
}

// ParentGatewayReference identifies the Gateway a ListenerSet attaches its listeners to. Unlike
// the general-purpose ParentReference (which can point at a Gateway, a Service, or other kinds),
// this is scoped to Gateway-shaped referents only, matching the live CRD schema.
type ParentGatewayReference struct {
	// Group is the group of the referent.
	//
	// +optional
	// +kubebuilder:default=gateway.networking.k8s.io
	Group *Group `json:"group,omitempty"`

	// Kind is the kind of the referent.
	//
	// +optional
	// +kubebuilder:default=Gateway
	Kind *Kind `json:"kind,omitempty"`

	// Name is the name of the referent.
	Name ObjectName `json:"name"`

	// Namespace is the namespace of the referent. When unspecified, this refers to the local
	// namespace of the ListenerSet.
	//
	// +optional
	Namespace *Namespace `json:"namespace,omitempty"`
}

// ListenerSetStatus defines the observed state of ListenerSet.
type ListenerSetStatus struct {
	// Conditions describe the current conditions of the ListenerSet, following the standard
	// Kubernetes conventions.
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=8
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}
