package pkg

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/f5devcentral/bigip-kubernetes-gateway/internal/k8s"
	"github.com/f5devcentral/f5-bigip-rest-go/utils"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	gatewayapi "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (c *SIGCache) SetNamespace(obj *v1.Namespace) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if obj != nil {
		c.Namespace[obj.Name] = obj
	}
}

func (c *SIGCache) UnsetNamespace(keyname string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.Namespace, keyname)
}

func (c *SIGCache) GetNamespace(keyname string) *v1.Namespace {
	// Was Lock()/Unlock() (a write lock) for a pure read -- confirmed live as one half of a real
	// deadlock: this is called extremely frequently (every parseGateway pass, every RouteMatches
	// check), and Go's sync.RWMutex blocks ALL new readers the moment any writer is queued, so an
	// unnecessary write-lock request here regularly queued up behind (and got queued behind by)
	// other goroutines' legitimate read locks, including one nested inside another already-held
	// read lock elsewhere in this cache (see hrParentName/_hrParentName's own doc comment for that
	// other half). Read-only access only ever needs RLock.
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.Namespace[keyname]
}

func (c *SIGCache) SetGatewayClass(obj *gatewayapi.GatewayClass) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if obj != nil {
		c.GatewayClass[obj.Name] = obj
	}
}

func (c *SIGCache) UnsetGatewayClass(keyname string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.GatewayClass, keyname)
}

func (c *SIGCache) GetGatewayClass(keyname string) *gatewayapi.GatewayClass {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.GatewayClass[keyname]
}

func (c *SIGCache) SetGateway(obj *gatewayapi.Gateway) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if obj != nil {
		c.Gateway[utils.Keyname(obj.Namespace, obj.Name)] = obj
	}
}

func (c *SIGCache) UnsetGateway(keyname string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.Gateway, keyname)
}

func (c *SIGCache) GetGateway(keyname string) *gatewayapi.Gateway {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.Gateway[keyname]
}

func (c *SIGCache) SetListenerSet(obj *gatewayapi.ListenerSet) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if obj != nil {
		c.ListenerSet[utils.Keyname(obj.Namespace, obj.Name)] = obj
	}
}

func (c *SIGCache) UnsetListenerSet(keyname string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.ListenerSet, keyname)
}

func (c *SIGCache) GetListenerSet(keyname string) *gatewayapi.ListenerSet {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.ListenerSet[keyname]
}

func (c *SIGCache) SetHTTPRoute(obj *gatewayapi.HTTPRoute) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if obj != nil {
		c.HTTPRoute[utils.Keyname(obj.Namespace, obj.Name)] = obj
	}
}

func (c *SIGCache) UnsetHTTPRoute(keyname string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.HTTPRoute, keyname)
}

func (c *SIGCache) GetHTTPRoute(keyname string) *gatewayapi.HTTPRoute {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.HTTPRoute[keyname]
}

func (c *SIGCache) GetService(keyname string) *v1.Service {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.Service[keyname]
}

func (c *SIGCache) GetServicesWithNamespace(namespace string) []*v1.Service {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	rlt := []*v1.Service{}
	for _, svc := range c.Service {
		if svc.Namespace == namespace {
			rlt = append(rlt, svc)
		}
	}
	return rlt
}

func (c *SIGCache) GetEndpoints(keyname string) *v1.Endpoints {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.Endpoints[keyname]
}

func (c *SIGCache) SetEndpoints(eps *v1.Endpoints) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if eps != nil {
		c.Endpoints[utils.Keyname(eps.Namespace, eps.Name)] = eps
	}
}
func (c *SIGCache) UnsetEndpoints(keyname string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.Endpoints, keyname)
}

func (c *SIGCache) SetService(svc *v1.Service) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if svc != nil {
		c.Service[utils.Keyname(svc.Namespace, svc.Name)] = svc
	}
}
func (c *SIGCache) UnsetService(keyname string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.Service, keyname)
}

func (c *SIGCache) SetSecret(scrt *v1.Secret) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	keyname := utils.Keyname(scrt.Namespace, scrt.Name)
	c.Secret[keyname] = scrt
}

func (c *SIGCache) UnsetSerect(keyname string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.Secret, keyname)
}

func (c *SIGCache) GetSecret(keyname string) *v1.Secret {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.Secret[keyname]
}

func (c *SIGCache) SetReferenceGrant(rg *gatewayv1beta1.ReferenceGrant) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c._setReferenceGrant(rg)
}

func (c *SIGCache) _setReferenceGrant(rg *gatewayv1beta1.ReferenceGrant) {
	if rg != nil {
		keyname := utils.Keyname(rg.Namespace, rg.Name)
		if org, ok := c.ReferenceGrant[keyname]; ok {
			refFromTo.unset(org)
		}
		c.ReferenceGrant[keyname] = rg
		refFromTo.set(rg)
	}
}

func (c *SIGCache) UnsetReferenceGrant(keyname string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	rg := c.ReferenceGrant[keyname]
	if rg != nil {
		refFromTo.unset(rg)
		delete(c.ReferenceGrant, keyname)
	}
}

func (c *SIGCache) GetReferenceGrant(keyname string) *gatewayv1beta1.ReferenceGrant {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.ReferenceGrant[keyname]
}

func (c *SIGCache) AttachedGateways(gwc *gatewayapi.GatewayClass) []*gatewayapi.Gateway {
	defer utils.TimeItToPrometheus()()

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c._attachedGateways(gwc)
}

func (c *SIGCache) _attachedGateways(gwc *gatewayapi.GatewayClass) []*gatewayapi.Gateway {
	if gwc == nil {
		return []*gatewayapi.Gateway{}
	}

	gws := []*gatewayapi.Gateway{}
	for _, gw := range c.Gateway {
		if gw.Spec.GatewayClassName == gatewayapi.ObjectName(gwc.Name) {
			gws = append(gws, gw)
		}
	}
	return gws
}

func (c *SIGCache) GatewayRefsOfHR(hr *gatewayapi.HTTPRoute) []*gatewayapi.Gateway {
	defer utils.TimeItToPrometheus()()

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c._gatewayRefsOfHR(hr)
}

func (c *SIGCache) _gatewayRefsOfHR(hr *gatewayapi.HTTPRoute) []*gatewayapi.Gateway {
	defer utils.TimeItToPrometheus()()

	if hr == nil {
		return []*gatewayapi.Gateway{}
	}
	gws := []*gatewayapi.Gateway{}
	for i := range hr.Spec.ParentRefs {
		if gw, attached := c._parentRefTarget(hr, &hr.Spec.ParentRefs[i]); attached {
			gws = append(gws, gw)
		}
	}
	return gws
}

// ParentRefTarget resolves one of hr's parentRefs (a Gateway, or a ListenerSet standing in for
// its parent Gateway) to the Gateway it ultimately targets -- nil if that isn't cached -- and
// whether any listener it names actually accepts this route (sectionName, allowedRoutes).
func (c *SIGCache) ParentRefTarget(hr *gatewayapi.HTTPRoute, pr *gatewayapi.ParentReference) (*gatewayapi.Gateway, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c._parentRefTarget(hr, pr)
}

func (c *SIGCache) _parentRefTarget(hr *gatewayapi.HTTPRoute, pr *gatewayapi.ParentReference) (*gatewayapi.Gateway, bool) {
	ns := hr.Namespace
	if pr.Namespace != nil {
		ns = string(*pr.Namespace)
	}

	// A parentRef can target a Gateway directly, or a ListenerSet attached to one (see
	// ListenerSet's own doc comment) -- resolve to the actual Gateway either way, and check
	// against whichever object's own listeners the parentRef's sectionName is scoped to (the
	// ListenerSet's own, not the Gateway's, when going through one). refNamespace is what
	// RouteMatches treats as "the Gateway's own namespace" for AllowedRoutes' "Same" case --
	// per ListenerSet's own ReferenceGrant-semantics doc comment ("A ListenerSet can
	// reference secrets/backends in its own namespace without a ReferenceGrant"), that's the
	// ListenerSet's namespace, not necessarily the Gateway's, when going through one.
	var gw *gatewayapi.Gateway
	var candidateListeners []gatewayapi.Listener
	refNamespace := ""
	if pr.Kind != nil && string(*pr.Kind) == "ListenerSet" {
		if ls := c.ListenerSet[utils.Keyname(ns, string(pr.Name))]; ls != nil {
			gw = c._gatewayRefOfListenerSet(ls)
			candidateListeners = ls.Spec.Listeners
			refNamespace = ls.Namespace
		}
	} else if candidate, ok := c.Gateway[utils.Keyname(ns, string(pr.Name))]; ok {
		gw = candidate
		candidateListeners = candidate.Spec.Listeners
		refNamespace = candidate.Namespace
	}
	if gw == nil {
		return nil, false
	}

	for _, listener := range candidateListeners {
		// SectionName is optional per the Gateway API spec -- nil means "matches any
		// listener", not "matches none". Dereferencing it unconditionally here panicked
		// (nil pointer deref) for ANY HTTPRoute anywhere in the cluster whose parentRef
		// omits it, which is normal/valid and is how every Cilium-targeted HTTPRoute in
		// this repo is written (this controller's own parser.go requires sectionName for
		// routes attaching to ITS OWN GatewayClass, but this cache-lookup path runs for
		// every Service's Endpoints changes cluster-wide, regardless of GatewayClass, so
		// it can't assume that same constraint holds). Confirmed live: crashed the pod
		// (CrashLoopBackOff) on an Endpoints event for a Service wholly unrelated to
		// this controller's own Gateways.
		if pr.SectionName != nil && listener.Name != *pr.SectionName {
			continue
		}
		routetype := reflect.TypeOf(*hr).Name()
		if RouteMatches(refNamespace, &listener, c.Namespace[hr.Namespace], routetype) {
			return gw, true
		}
	}
	return gw, false
}

// HTTPRoutesReferencingClass returns every cached HTTPRoute with at least one parentRef that
// resolves to a Gateway of class className -- attached or not, so status can also report routes
// a listener refuses (NotAllowedByListeners), which AttachedHTTPRoutes leaves out.
func (c *SIGCache) HTTPRoutesReferencingClass(className string) []*gatewayapi.HTTPRoute {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	hrs := []*gatewayapi.HTTPRoute{}
	for _, hr := range c.HTTPRoute {
		for i := range hr.Spec.ParentRefs {
			if gw, _ := c._parentRefTarget(hr, &hr.Spec.ParentRefs[i]); gw != nil && string(gw.Spec.GatewayClassName) == className {
				hrs = append(hrs, hr)
				break
			}
		}
	}
	return hrs
}

// ListenerSetsReferencingGateway returns every cached ListenerSet whose parentRef names gw,
// whether or not gw's allowedListeners actually admits it (AttachedListenerSets decides that).
func (c *SIGCache) ListenerSetsReferencingGateway(gw *gatewayapi.Gateway) []*gatewayapi.ListenerSet {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	lsets := []*gatewayapi.ListenerSet{}
	for _, ls := range c.ListenerSet {
		if target := c._gatewayRefOfListenerSet(ls); target != nil &&
			target.Namespace == gw.Namespace && target.Name == gw.Name {
			lsets = append(lsets, ls)
		}
	}
	return lsets
}

// GatewayRefOfListenerSet resolves a ListenerSet's spec.parentRef (unlike an HTTPRoute, a
// ListenerSet has exactly one -- it can only ever attach to one Gateway) to that Gateway object,
// or nil if it doesn't resolve to a Gateway this cache knows about.
func (c *SIGCache) GatewayRefOfListenerSet(ls *gatewayapi.ListenerSet) *gatewayapi.Gateway {
	defer utils.TimeItToPrometheus()()

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c._gatewayRefOfListenerSet(ls)
}

func (c *SIGCache) _gatewayRefOfListenerSet(ls *gatewayapi.ListenerSet) *gatewayapi.Gateway {
	if ls == nil {
		return nil
	}
	pr := ls.Spec.ParentRef
	ns := ls.Namespace
	if pr.Namespace != nil {
		ns = string(*pr.Namespace)
	}
	return c.Gateway[utils.Keyname(ns, string(pr.Name))]
}

// listenerRefersSecret reports whether listener's TLS config references scrt, checking it against
// referrer for ReferenceGrant purposes (the Gateway itself for its own listeners, or the specific
// ListenerSet for one it contributes -- see AttachedSecrets' identical referrer reasoning).
func (c *SIGCache) listenerRefersSecret(listener *gatewayapi.Listener, referrer client.Object, scrt *v1.Secret) (bool, error) {
	if listener.Protocol != gatewayapi.HTTPSProtocolType {
		return false, nil
	}
	if listener.TLS == nil {
		return false, fmt.Errorf("invalid tls setting in listener")
	}
	if listener.TLS.Mode != nil && *listener.TLS.Mode != gatewayapi.TLSModeTerminate {
		return false, nil
	}
	for _, ref := range listener.TLS.CertificateRefs {
		ns := referrer.GetNamespace()
		if ref.Namespace != nil {
			ns = string(*ref.Namespace)
		}
		if ns == scrt.Namespace && ref.Name == gatewayapi.ObjectName(scrt.Name) {
			if err := validateSecretType(ref.Group, ref.Kind); err != nil {
				return false, err
			}
			if !c._canRefer(referrer, scrt) {
				return false, fmt.Errorf("'%s' cannot refer to secret '%s'",
					utils.Keyname(referrer.GetNamespace(), referrer.GetName()), utils.Keyname(scrt.Namespace, scrt.Name))
			}
			return true, nil
		}
	}
	return false, nil
}

func (c *SIGCache) GatewayRefsOfSecret(scrt *v1.Secret) ([]*gatewayapi.Gateway, error) {
	defer utils.TimeItToPrometheus()()

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if scrt == nil {
		return []*gatewayapi.Gateway{}, nil
	}
	gws := []*gatewayapi.Gateway{}

	for _, gw := range c.Gateway {
		found := false
		for i := 0; i < len(gw.Spec.Listeners) && !found; i++ {
			matched, err := c.listenerRefersSecret(&gw.Spec.Listeners[i], gw, scrt)
			if err != nil {
				return gws, err
			}
			found = matched
		}
		// A secret referenced only by a ListenerSet's own listener (not the Gateway's own) --
		// e.g. a per-tenant Certificate attached via ListenerSet, the common case for anything
		// using EffectiveListeners' ListenerSet-merge support -- used to never match here, since
		// this loop only ever checked gw.Spec.Listeners. That meant SecretReconciler resolved
		// zero impacted Gateways for it and never triggered a redeploy on create/rotate: the cert
		// existed and was even correctly picked up whenever some OTHER event happened to trigger
		// a redeploy of the same class, but the secret's OWN create event did nothing on its own.
		// Confirmed live: kasten's dedicated Certificate issued successfully, but the shared HTTPS
		// Virtual Server kept serving the wildcard cert (the only one whose Gateway resolved here)
		// until something unrelated forced another redeploy.
		if !found {
			for _, lset := range c._attachedListenerSets(gw) {
				for i := 0; i < len(lset.Spec.Listeners) && !found; i++ {
					matched, err := c.listenerRefersSecret(&lset.Spec.Listeners[i], lset, scrt)
					if err != nil {
						return gws, err
					}
					found = matched
				}
				if found {
					break
				}
			}
		}
		if found {
			gws = append(gws, gw)
		}
	}

	return gws, nil
}

// AttachedSecrets returns each listener's resolved certs (keyed by vsname, same as
// EffectiveListeners), plus a second map naming which vsnames have at least one CertificateRef
// that couldn't be resolved -- a secret that doesn't exist (yet) or can't be referenced. That
// second condition is deliberately NOT an error: it's the expected, normal state of a listener
// whose cert-manager Certificate is still being issued (Certificate -> CertificateRequest ->
// Order -> Challenge -> Secret is not instantaneous). Confirmed live: a listener in this state
// used to make this function return a hard error, which parseGateway propagated, which
// ParseAllForClass propagated further -- aborting the ENTIRE GatewayClass's deployment, not just
// the one listener with a pending cert. Every other Gateway and listener in the class silently
// stopped receiving new changes (though nothing already deployed got wiped, thanks to
// AS3Deployer's tenantCache seeding) until that one cert finished issuing. The caller
// (parseGateway) uses the incomplete map to skip building a Virtual Server for a listener/group
// that ended up with no usable cert, instead of every listener sharing its GatewayClass.
//
// Malformed listener config (missing spec.TLS, or a CertificateRef of the wrong Secret type) is
// still a hard error -- those are permanent misconfigurations that retrying won't fix, unlike a
// cert mid-issuance.
func (c *SIGCache) AttachedSecrets(gw *gatewayapi.Gateway) (map[string][]*v1.Secret, map[string]bool, error) {
	defer utils.TimeItToPrometheus()()

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	rlt := map[string][]*v1.Secret{}
	incomplete := map[string]bool{}
	if gw == nil {
		return rlt, incomplete, nil
	}

	// referrer is whichever object actually declared a given listener -- the Gateway itself
	// for its own listeners, or the specific ListenerSet for one it contributes. This matters
	// for cross-namespace ReferenceGrant resolution (_canRefer's "from" side): per ListenerSet's
	// own doc comment, "ReferenceGrants applied to a Gateway are not inherited by child
	// ListenerSets" and vice versa -- each is judged on its own namespace/identity, not the
	// other's.
	collect := func(vsname string, listener *gatewayapi.Listener, referrer client.Object) error {
		if _, ok := rlt[vsname]; !ok {
			rlt[vsname] = []*v1.Secret{}
		}
		if listener.Protocol != gatewayapi.HTTPSProtocolType {
			return nil
		}
		if listener.TLS == nil {
			return fmt.Errorf("invalid listener.TLS setting")
		}
		if listener.TLS.Mode != nil && *listener.TLS.Mode != gatewayapi.TLSModeTerminate {
			return nil
		}
		for _, ref := range listener.TLS.CertificateRefs {
			ns := referrer.GetNamespace()
			if ref.Namespace != nil {
				ns = string(*ref.Namespace)
			}
			n := utils.Keyname(ns, string(ref.Name))
			scrt := c.Secret[n]
			if scrt == nil || !c._canRefer(referrer, scrt) {
				incomplete[vsname] = true
				continue
			}
			if err := validateSecretType(ref.Group, ref.Kind); err != nil {
				return err
			}
			rlt[vsname] = append(rlt[vsname], scrt)
		}
		return nil
	}

	for i, listener := range gw.Spec.Listeners {
		if err := collect(gwListenerName(gw, &listener), &gw.Spec.Listeners[i], gw); err != nil {
			return rlt, incomplete, err
		}
	}
	for _, lset := range c._attachedListenerSets(gw) {
		for i, listener := range lset.Spec.Listeners {
			if err := collect(listenerSetEntryName(gw, lset, listener.Name), &lset.Spec.Listeners[i], lset); err != nil {
				return rlt, incomplete, err
			}
		}
	}
	return rlt, incomplete, nil
}

func (c *SIGCache) AttachedHTTPRoutes(gw *gatewayapi.Gateway) []*gatewayapi.HTTPRoute {
	defer utils.TimeItToPrometheus()()

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c._attachedHTTPRoutes(gw)
}

func (c *SIGCache) _attachedHTTPRoutes(gw *gatewayapi.Gateway) []*gatewayapi.HTTPRoute {
	if gw == nil {
		return []*gatewayapi.HTTPRoute{}
	}

	listeners := c._effectiveListeners(gw)

	hrs := []*gatewayapi.HTTPRoute{}
	for _, hr := range c.HTTPRoute {
		for _, pr := range hr.Spec.ParentRefs {
			ns := hr.Namespace
			if pr.Namespace != nil {
				ns = string(*pr.Namespace)
			}

			// A parentRef can target gw directly, or a ListenerSet attached to it -- see
			// _gatewayRefsOfHR's identical reasoning just above for why refNamespace (what
			// RouteMatches treats as "the Gateway's own namespace" for AllowedRoutes' "Same"
			// case) is the ListenerSet's own namespace, not gw's, when going through one.
			refNamespace := ""
			targetsGW := false
			if pr.Kind != nil && string(*pr.Kind) == "ListenerSet" {
				if ls := c.ListenerSet[utils.Keyname(ns, string(pr.Name))]; ls != nil {
					if target := c._gatewayRefOfListenerSet(ls); target != nil &&
						utils.Keyname(target.Namespace, target.Name) == utils.Keyname(gw.Namespace, gw.Name) {
						targetsGW = true
						refNamespace = ls.Namespace
					}
				}
			} else if utils.Keyname(ns, string(pr.Name)) == utils.Keyname(gw.Namespace, gw.Name) {
				targetsGW = true
				refNamespace = gw.Namespace
			}
			if !targetsGW {
				continue
			}

			vsname := c._hrParentName(hr, &pr)
			routeNamespace := c.Namespace[hr.Namespace]
			routetype := reflect.TypeOf(*hr).Name()
			if RouteMatches(refNamespace, listeners[vsname], routeNamespace, routetype) {
				hrs = append(hrs, hr)
				break
			}
		}
	}

	// c.HTTPRoute is a map, so the range above visits routes in Go's randomized iteration order --
	// and that order is exactly the order their generated iRules get appended onto a shared
	// Virtual Server (see parser.go's "irules mapping: for httproutes" and the merge loop below
	// it), which BIG-IP then runs in sequence for every request. A route with a broad match (e.g.
	// a catch-all HTTP->HTTPS redirect matching any path under a wildcard host) landing BEFORE a
	// more specific one (e.g. an ACME HTTP-01 solver's exact challenge-path match) means the
	// broad rule responds first and the specific one never gets a chance, on whichever deploys
	// happen to iterate the map that way -- confirmed live: the exact same solver route worked on
	// one deploy and 301-redirected (never served the challenge) on the next, with no config
	// change in between, purely from map iteration order flipping. Sorting by match specificity
	// (routeSpecificity below) before returning makes the order the SAME every time, and makes the
	// more specific match win, consistent with the Gateway API spec's own HTTPRoute precedence
	// rules (exact > longer prefix > shorter prefix).
	sort.SliceStable(hrs, func(i, j int) bool {
		return routeSpecificity(hrs[i]) > routeSpecificity(hrs[j])
	})
	return hrs
}

// routeSpecificity scores an HTTPRoute for ordering multiple routes' generated iRules on a
// shared Virtual Server so the more specific one runs first (see _attachedHTTPRoutes' own doc
// comment on why order matters here). Hostname specificity dominates (an exact hostname always
// outranks a wildcard one, which always outranks no hostname at all -- i.e. "matches any host"),
// with path specificity only breaking ties between routes at the same hostname tier. Without the
// hostname component, a wildcard-hostname catch-all (e.g. a global "page not found" route meant
// to catch anything not matched by a real app) would sort relative to every real app's
// exact-hostname route in Go's randomized map iteration order -- on an unlucky deploy it could
// land BEFORE one and silently swallow that app's traffic, the exact failure mode this function
// already exists to prevent for path matches (see the ACME solver incident in
// _attachedHTTPRoutes' comment).
func routeSpecificity(hr *gatewayapi.HTTPRoute) int {
	return hostnameSpecificity(hr.Spec.Hostnames)<<40 + pathSpecificity(hr)
}

// hostnameSpecificity scores a route's most specific hostname: exact > wildcard > none (no
// Hostnames at all means "matches any host the Gateway/listener allows", the least specific
// case). Mirrors Gateway API's own hostname precedence rules.
func hostnameSpecificity(hostnames []gatewayapi.Hostname) int {
	best := 0
	for _, h := range hostnames {
		score := 2
		if strings.HasPrefix(string(h), "*.") {
			score = 1
		}
		if score > best {
			best = score
		}
	}
	return best
}

// pathSpecificity scores an HTTPRoute by its most specific path match. An Exact match outranks
// any PathPrefix regardless of length; among PathPrefix matches, a longer prefix is more
// specific. Mirrors Gateway API's own precedence rules for HTTPRoute path matches.
func pathSpecificity(hr *gatewayapi.HTTPRoute) int {
	best := 0
	for _, rule := range hr.Spec.Rules {
		for _, m := range rule.Matches {
			if m.Path == nil {
				continue
			}
			score := 0
			switch {
			case m.Path.Type != nil && *m.Path.Type == gatewayapi.PathMatchExact:
				score = 1<<30 + len(strPtrVal(m.Path.Value))
			case m.Path.Value != nil:
				score = len(*m.Path.Value)
			}
			if score > best {
				best = score
			}
		}
	}
	return best
}

func strPtrVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// EffectiveListeners returns every listener that's actually part of gw's served configuration --
// its own spec.Listeners, plus every listener contributed by a ListenerSet attached to it (see
// AttachedListenerSets) -- keyed by the same name parseGateway and hrParentName use for that
// listener (gwListenerName for gw's own, listenerSetEntryName for a ListenerSet's), so a route's
// parentRef and this merged view always agree on what a given listener actually is.
func (c *SIGCache) EffectiveListeners(gw *gatewayapi.Gateway) map[string]*gatewayapi.Listener {
	defer utils.TimeItToPrometheus()()

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c._effectiveListeners(gw)
}

func (c *SIGCache) _effectiveListeners(gw *gatewayapi.Gateway) map[string]*gatewayapi.Listener {
	listeners := map[string]*gatewayapi.Listener{}
	if gw == nil {
		return listeners
	}
	for i, ls := range gw.Spec.Listeners {
		listeners[gwListenerName(gw, &ls)] = &gw.Spec.Listeners[i]
	}
	for _, lset := range c._attachedListenerSets(gw) {
		for i, ls := range lset.Spec.Listeners {
			listeners[listenerSetEntryName(gw, lset, ls.Name)] = &lset.Spec.Listeners[i]
		}
	}
	return listeners
}

func (c *SIGCache) AttachedListenerSets(gw *gatewayapi.Gateway) []*gatewayapi.ListenerSet {
	defer utils.TimeItToPrometheus()()

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c._attachedListenerSets(gw)
}

// _attachedListenerSets returns the ListenerSets that are both correctly parented to gw (their
// spec.parentRef resolves to it) AND permitted by gw's own spec.AllowedListeners -- a Gateway
// allows no ListenerSets by default (AllowedListeners.Namespaces.From defaults to "None"), so an
// otherwise-correctly-parented ListenerSet in a Gateway that never opted in is NOT attached.
// Mirrors RouteMatches' own From-namespace matching (utils.go) for the "Selector" case.
func (c *SIGCache) _attachedListenerSets(gw *gatewayapi.Gateway) []*gatewayapi.ListenerSet {
	if gw == nil || gw.Spec.AllowedListeners == nil || gw.Spec.AllowedListeners.Namespaces == nil {
		return []*gatewayapi.ListenerSet{}
	}
	allowedNS := gw.Spec.AllowedListeners.Namespaces
	from := gatewayapi.NamespacesFromNone
	if allowedNS.From != nil {
		from = *allowedNS.From
	}
	if from == gatewayapi.NamespacesFromNone {
		return []*gatewayapi.ListenerSet{}
	}

	lsets := []*gatewayapi.ListenerSet{}
	for _, ls := range c.ListenerSet {
		pr := ls.Spec.ParentRef
		// Namespace omitted on the parentRef means "same namespace as the referring
		// ListenerSet" (standard Gateway API convention for parentRefs), not the target
		// Gateway's namespace.
		parentNS := ls.Namespace
		if pr.Namespace != nil {
			parentNS = string(*pr.Namespace)
		}
		if parentNS != gw.Namespace || string(pr.Name) != gw.Name {
			continue
		}

		switch from {
		case gatewayapi.NamespacesFromAll:
			lsets = append(lsets, ls)
		case gatewayapi.NamespacesFromSame:
			if ls.Namespace == gw.Namespace {
				lsets = append(lsets, ls)
			}
		case gatewayapi.NamespacesFromSelector:
			selector, err := metav1.LabelSelectorAsSelector(allowedNS.Selector)
			if err != nil {
				continue
			}
			if nsObj := c.Namespace[ls.Namespace]; nsObj != nil && selector.Matches(labels.Set(nsObj.Labels)) {
				lsets = append(lsets, ls)
			}
		}
	}
	return lsets
}

func (c *SIGCache) AttachedServices(hr *gatewayapi.HTTPRoute) []*v1.Service {
	defer utils.TimeItToPrometheus()()

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c._attachedServices(hr)
}

func (c *SIGCache) _attachedServices(hr *gatewayapi.HTTPRoute) []*v1.Service {
	if hr == nil {
		return []*v1.Service{}
	}

	svcs := []*v1.Service{}
	for _, rl := range hr.Spec.Rules {
		for _, br := range rl.BackendRefs {
			ns := hr.Namespace
			if br.Namespace != nil {
				ns = string(*br.Namespace)
			}
			if svc, ok := c.Service[utils.Keyname(ns, string(br.Name))]; ok && c._canRefer(hr, svc) {
				svcs = append(svcs, svc)
			}
		}
	}
	for _, rl := range hr.Spec.Rules {
		for _, fl := range rl.Filters {
			if fl.Type == gatewayapi.HTTPRouteFilterExtensionRef && fl.ExtensionRef != nil {
				er := fl.ExtensionRef
				if er.Group == "" && er.Kind == "Service" {
					if svc, ok := c.Service[utils.Keyname(hr.Namespace, string(er.Name))]; ok && c._canRefer(hr, svc) {
						svcs = append(svcs, svc)
					}
				}
			}
		}
	}
	return svcs
}

// func (c *SIGCache) AllAttachedServiceKeys() []string {
// 	defer utils.TimeItToPrometheus()()

// 	c.mutex.RLock()
// 	defer c.mutex.RUnlock()

// 	svcs := []*v1.Service{}
// 	for _, gwc := range c.GatewayClass {
// 		for _, gw := range c._attachedGateways(gwc) {
// 			for _, hr := range c._attachedHTTPRoutes(gw) {
// 				svcs = append(svcs, c._attachedServices(hr)...)
// 			}
// 		}
// 	}

// 	rlt := []string{}
// 	for _, svc := range svcs {
// 		rlt = append(rlt, utils.Keyname(svc.Namespace, svc.Name))
// 	}
// 	return rlt
// }

func (c *SIGCache) RelatedServices(gwc *gatewayapi.GatewayClass) []*v1.Service {
	defer utils.TimeItToPrometheus()()

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	svcs := []*v1.Service{}
	for _, gw := range c._attachedGateways(gwc) {
		for _, hr := range c._attachedHTTPRoutes(gw) {
			svcs = append(svcs, c._attachedServices(hr)...)
		}
	}

	return svcs
}

func (c *SIGCache) AllHTTPRoutes() []*gatewayapi.HTTPRoute {
	defer utils.TimeItToPrometheus()()

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	hrs := []*gatewayapi.HTTPRoute{}
	for _, hr := range c.HTTPRoute {
		hrs = append(hrs, hr)
	}
	return hrs
}

func (c *SIGCache) HTTPRoutesRefsOf(svc *v1.Service) []*gatewayapi.HTTPRoute {
	defer utils.TimeItToPrometheus()()

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c._HTTPRoutesRefsOf(svc)
}

func (c *SIGCache) _HTTPRoutesRefsOf(svc *v1.Service) []*gatewayapi.HTTPRoute {
	if svc == nil {
		return []*gatewayapi.HTTPRoute{}
	}

	// To performance perspective, it may be good.
	// But the implementation is similiar to _attachedServices, would easily introduce issues.
	// refered := func(hr *gatewayapi.HTTPRoute) bool {
	// 	for _, rl := range hr.Spec.Rules {
	// 		for _, br := range rl.BackendRefs {
	// 			ns := hr.Namespace
	// 			if br.Namespace != nil {
	// 				ns = string(*br.Namespace)
	// 			}
	// 			if utils.Keyname(ns, string(br.Name)) == utils.Keyname(svc.Namespace, svc.Name) {
	// 				return true
	// 			}
	// 		}
	// 	}
	// 	for _, rl := range hr.Spec.Rules {
	// 		for _, fl := range rl.Filters {
	// 			if fl.Type == gatewayapi.HTTPRouteFilterExtensionRef && fl.ExtensionRef != nil {
	// 				er := fl.ExtensionRef
	// 				if er.Group == "" && er.Kind == "Service" {
	// 					if utils.Keyname(hr.Namespace, string(er.Name)) == utils.Keyname(svc.Namespace, svc.Name) {
	// 						return true
	// 					}
	// 				}
	// 			}
	// 		}
	// 	}
	// 	return false
	// }

	hrKeys := []string{}
	for _, hr := range c.HTTPRoute {
		svcs := c._attachedServices(hr)
		svcKeys := []string{}
		for _, s := range svcs {
			svcKeys = append(svcKeys, utils.Keyname(s.Namespace, s.Name))
		}
		if utils.Contains(svcKeys, utils.Keyname(svc.Namespace, svc.Name)) {
			hrKeys = append(hrKeys, utils.Keyname(hr.Namespace, hr.Name))
		}
	}
	hrKeys = utils.Unified(hrKeys)

	hrs := []*gatewayapi.HTTPRoute{}
	for _, hrk := range hrKeys {
		hrs = append(hrs, c.HTTPRoute[hrk])
	}

	return hrs
}

// GetNeighborGateways get neighbor gateways(itself is not included) for all gateway class.
func (c *SIGCache) GetNeighborGateways(gw *gatewayapi.Gateway) []*gatewayapi.Gateway {
	defer utils.TimeItToPrometheus()()

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	gwmap := map[string]*gatewayapi.Gateway{}
	hrs := c._attachedHTTPRoutes(gw)
	for _, hr := range hrs {
		gws := c._gatewayRefsOfHR(hr)
		for _, ng := range gws {
			kn := utils.Keyname(ng.Namespace, ng.Name)
			if _, f := gwmap[kn]; !f {
				gwmap[kn] = ng
			}
		}
	}

	delete(gwmap, utils.Keyname(gw.Namespace, gw.Name))
	rlt := []*gatewayapi.Gateway{}
	for _, gw := range gwmap {
		rlt = append(rlt, gw)
	}

	return rlt
}

func (c *SIGCache) GetRootGateways(svcs []*v1.Service) []*gatewayapi.Gateway {
	defer utils.TimeItToPrometheus()()

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	gwmap := map[string]*gatewayapi.Gateway{}

	for _, svc := range svcs {
		hrs := c._HTTPRoutesRefsOf(svc)
		for _, hr := range hrs {
			gws := c._gatewayRefsOfHR(hr)
			for _, gw := range gws {
				gwmap[utils.Keyname(gw.Namespace, gw.Name)] = gw
			}
		}
	}
	rlt := []*gatewayapi.Gateway{}
	for _, gw := range gwmap {
		rlt = append(rlt, gw)
	}
	return rlt
}

func (c *SIGCache) RGImpactedGatewayClasses(rg *gatewayv1beta1.ReferenceGrant) []string {
	defer utils.TimeItToPrometheus()()

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	hrs := c._rgImpactedHTTPRoutes(rg)
	gws := c._rgImpactedGateways(rg)
	for _, hr := range hrs {
		gws = append(gws, c._gatewayRefsOfHR(hr)...)
	}
	gws = unifiedGateways(gws)
	return classNamesOfGateways(gws)
}

func (c *SIGCache) NSImpactedGatewayClasses(ns *v1.Namespace) []string {
	defer utils.TimeItToPrometheus()()

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if ns == nil {
		return []string{}
	}

	names := []string{}
	for _, hr := range c.HTTPRoute {
		if hr.Namespace == ns.Name {
			for _, pr := range hr.Spec.ParentRefs {
				ns := hr.Namespace
				if pr.Namespace != nil {
					ns = string(*pr.Namespace)
				}
				name := utils.Keyname(utils.Keyname(ns, string(pr.Name)))
				if gw, ok := c.Gateway[name]; ok {
					names = append(names, string(gw.Spec.GatewayClassName))
				}
			}
		}
	}

	return utils.Unified(names)
}

func (c *SIGCache) _rgImpactedGateways(rg *gatewayv1beta1.ReferenceGrant) []*gatewayapi.Gateway {
	defer utils.TimeItToPrometheus()()

	rlt := []*gatewayapi.Gateway{}
	if rg == nil {
		return rlt
	}

	for _, f := range rg.Spec.From {
		if gatewayapi.GroupName == f.Group &&
			reflect.TypeOf(gatewayapi.Gateway{}).Name() == string(f.Kind) {
			for _, gw := range c.Gateway {
				if gw.Namespace == string(f.Namespace) {
					rlt = append(rlt, gw)
				}
			}
		}
	}

	return rlt
}

func (c *SIGCache) _rgImpactedHTTPRoutes(rg *gatewayv1beta1.ReferenceGrant) []*gatewayapi.HTTPRoute {
	defer utils.TimeItToPrometheus()()

	rlt := []*gatewayapi.HTTPRoute{}
	if rg == nil {
		return rlt
	}

	for _, f := range rg.Spec.From {
		if gatewayapi.GroupName == f.Group &&
			reflect.TypeOf(gatewayapi.HTTPRoute{}).Name() == string(f.Kind) {
			for _, hr := range c.HTTPRoute {
				if hr.Namespace == string(f.Namespace) {
					rlt = append(rlt, hr)
				}
			}
		}
	}
	return rlt
}

// CanRefer parameter "from" and "to" MUST NOT be nil.
func (c *SIGCache) CanRefer(from, to client.Object) bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c._canRefer(from, to)
}

func (c *SIGCache) _canRefer(from, to client.Object) bool {
	fromns := client.Object.GetNamespace(from)
	tons := client.Object.GetNamespace(to)
	if fromns == tons {
		return true
	}

	fromgvk := client.Object.GetObjectKind(from).GroupVersionKind()
	if fromgvk.Group != gatewayapi.GroupName {
		return false
	}
	rgf := gatewayv1beta1.ReferenceGrantFrom{
		Group:     gatewayapi.Group(fromgvk.Group),
		Kind:      gatewayapi.Kind(fromgvk.Kind),
		Namespace: gatewayapi.Namespace(fromns),
	}
	f := stringifyRGFrom(&rgf)

	togvk := client.Object.GetObjectKind(to).GroupVersionKind()
	toname := gatewayapi.ObjectName(client.Object.GetName(to))
	rgt := gatewayv1beta1.ReferenceGrantTo{
		Group: gatewayapi.Group(togvk.Group),
		Kind:  gatewayapi.Kind(togvk.Kind),
		Name:  &toname,
	}
	t := stringifyRGTo(&rgt, tons)

	rgtAll := gatewayv1beta1.ReferenceGrantTo{
		Group: gatewayapi.Group(togvk.Group),
		Kind:  gatewayapi.Kind(togvk.Kind),
	}
	toAll := stringifyRGTo(&rgtAll, tons)

	return refFromTo.exists(f, t) || refFromTo.exists(f, toAll)
}

func (c *SIGCache) syncCoreV1Resources(mgr manager.Manager) error {
	defer utils.TimeItToPrometheus()()
	slog := utils.LogFromContext(context.TODO()).WithLevel(LogLevel)
	kubeClient, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("unable to create kubeclient: %s", err.Error())
	}

	if epsList, err := kubeClient.CoreV1().Endpoints(v1.NamespaceAll).List(context.TODO(), metav1.ListOptions{}); err != nil {
		return err
	} else {
		for _, eps := range epsList.Items {
			slog.Debugf("found eps %s", utils.Keyname(eps.Namespace, eps.Name))
			c.Endpoints[utils.Keyname(eps.Namespace, eps.Name)] = eps.DeepCopy()
		}
	}

	if svcList, err := kubeClient.CoreV1().Services(v1.NamespaceAll).List(context.TODO(), metav1.ListOptions{}); err != nil {
		return err
	} else {
		for _, svc := range svcList.Items {
			slog.Debugf("found svc %s", utils.Keyname(svc.Namespace, svc.Name))
			// listed services has no TypeMeta set, complement it for referenceGrant check.
			typed := svc.DeepCopy()
			typed.TypeMeta = metav1.TypeMeta{
				APIVersion: v1.SchemeGroupVersion.Version,
				Kind:       reflect.TypeOf(svc).Name(),
			}
			c.Service[utils.Keyname(svc.Namespace, svc.Name)] = typed
		}
	}

	if nsList, err := kubeClient.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{}); err != nil {
		return err
	} else {
		for _, ns := range nsList.Items {
			slog.Debugf("found ns: %s", ns.Name)
			c.Namespace[ns.Name] = ns.DeepCopy()
		}
	}

	if nList, err := kubeClient.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{}); err != nil {
		return err
	} else {
		for _, n := range nList.Items {
			// c.Node[n.Name] = n.DeepCopy()
			slog.Debugf("found node %s", n.Name)
			k8s.NodeCache.Set(&n)
		}
	}

	if secList, err := kubeClient.CoreV1().Secrets(v1.NamespaceAll).List(context.TODO(), metav1.ListOptions{}); err != nil {
		return err
	} else {
		for _, scrt := range secList.Items {
			slog.Debugf("found secret %s", scrt.Name)
			// listed secrets has no TypeMeta set, complement it for referenceGrant check.
			typed := scrt.DeepCopy()
			typed.TypeMeta = metav1.TypeMeta{
				APIVersion: v1.SchemeGroupVersion.Version,
				Kind:       reflect.TypeOf(scrt).Name(),
			}
			c.Secret[utils.Keyname(scrt.Namespace, scrt.Name)] = typed
		}
	}

	return nil
}

func (c *SIGCache) syncGatewayResources(mgr manager.Manager) error {
	defer utils.TimeItToPrometheus()()
	slog := utils.LogFromContext(context.TODO()).WithLevel(LogLevel)

	checkAndWaitCacheStarted := func() error {
		var gtwList gatewayapi.GatewayList
		for {
			if err := mgr.GetCache().List(context.TODO(), &gtwList, &client.ListOptions{}); err != nil {
				if reflect.DeepEqual(err, &cache.ErrCacheNotStarted{}) {
					slog.Debugf("Waiting for mgr cache to be ready.")
					<-time.After(100 * time.Millisecond)
				} else {
					return fmt.Errorf("failed to accessing mgr's cache: %s", err.Error())
				}
			} else {
				slog.Debugf("mgr cache is ready for syncing resources")
				break
			}
		}
		return nil
	}

	if err := checkAndWaitCacheStarted(); err != nil {
		return err
	}

	slog.Debugf("starting to sync resources")
	var gwcList gatewayapi.GatewayClassList
	var gtwList gatewayapi.GatewayList
	var hrList gatewayapi.HTTPRouteList
	var rgList gatewayv1beta1.ReferenceGrantList

	if err := mgr.GetCache().List(context.TODO(), &gwcList, &client.ListOptions{}); err != nil {
		return err
	} else {
		for _, gwc := range gwcList.Items {
			// Cache every GatewayClass here regardless of ownership -- matches the same fix (and
			// the same reasoning) in GatewayClassReconciler.Reconcile: ParseAllForClass and
			// HandleBackends need to tell "not cached yet" (transient, worth retrying) apart from
			// "cached, but belongs to a different controller" (permanent, correctly a no-op), and
			// this upfront sync used to only cache classes this controller owns, same bug as the
			// reconciler had -- meaning even after SyncedAtStart flipped true, a class belonging
			// to any OTHER controller (e.g. Cilium's cilium-private/cilium-public, both present in
			// this cluster) still wasn't cached, and anything reachable through it kept hitting
			// "not cached yet" until that class's own later, separate watch-triggered reconcile
			// happened to catch up. Confirmed live: the resulting retry storm coincided with (and
			// plausibly contributed to starving) dex/argocd/mygateway/f5-tenants staying
			// unreachable well past when SyncedAtStart itself had already gone true.
			slog.Debugf("found gatewayclass %s", gwc.Name)
			c.GatewayClass[gwc.Name] = gwc.DeepCopy()
		}
	}

	if err := mgr.GetCache().List(context.TODO(), &gtwList, &client.ListOptions{}); err != nil {
		return err
	} else {
		for _, gw := range gtwList.Items {
			slog.Debugf("found gateway %s", utils.Keyname(gw.Namespace, gw.Name))
			c.Gateway[utils.Keyname(gw.Namespace, gw.Name)] = gw.DeepCopy()
		}
	}

	if err := mgr.GetCache().List(context.TODO(), &hrList, &client.ListOptions{}); err != nil {
		return err
	} else {
		for _, hr := range hrList.Items {
			slog.Debugf("found httproute %s", utils.Keyname(hr.Namespace, hr.Name))
			c.HTTPRoute[utils.Keyname(hr.Namespace, hr.Name)] = hr.DeepCopy()
		}
	}

	if err := mgr.GetCache().List(context.TODO(), &rgList, &client.ListOptions{}); err != nil {
		return err
	} else {
		for _, rg := range rgList.Items {
			slog.Debugf("found referencegrant %s", utils.Keyname(rg.Namespace, rg.Name))
			c._setReferenceGrant(rg.DeepCopy())
		}
	}
	return nil
}

func (c *SIGCache) SyncAllResources(mgr manager.Manager) {
	defer utils.TimeItToPrometheus()()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	slog := utils.LogFromContext(context.TODO())
	if err := c.syncCoreV1Resources(mgr); err != nil {
		slog.Errorf("unable to sync k8s corev1 resources to local: %s", err.Error())
		os.Exit(1)
	}
	if err := c.syncGatewayResources(mgr); err != nil {
		slog.Errorf("failed to sync gateway api resources to local: %s", err.Error())
	}

	slog.Infof("Finished syncing resources to local")
	c.SyncedAtStart = true
}
