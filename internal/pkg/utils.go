package pkg

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"time"

	f5_bigip "github.com/f5devcentral/f5-bigip-rest-go/bigip"
	"github.com/f5devcentral/f5-bigip-rest-go/deployer"
	"github.com/f5devcentral/f5-bigip-rest-go/utils"
	"github.com/google/uuid"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	gatewayapi "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

func init() {
	ActiveSIGs = &SIGCache{
		mutex:          sync.RWMutex{},
		SyncedAtStart:  false,
		ControllerName: "",
		Gateway:        map[string]*gatewayapi.Gateway{},
		ListenerSet:    map[string]*gatewayapi.ListenerSet{},
		HTTPRoute:      map[string]*gatewayapi.HTTPRoute{},
		Endpoints:      map[string]*v1.Endpoints{},
		Service:        map[string]*v1.Service{},
		GatewayClass:   map[string]*gatewayapi.GatewayClass{},
		Namespace:      map[string]*v1.Namespace{},
		ReferenceGrant: map[string]*gatewayv1beta1.ReferenceGrant{},
		Secret:         map[string]*v1.Secret{},
	}
	refFromTo = &ReferenceGrantFromTo{}
	LogLevel = utils.LogLevel_Type_INFO
}

func hrName(hr *gatewayapi.HTTPRoute) string {
	return strings.Join([]string{"hr", hr.Namespace, hr.Name}, ".")
}

// poolName returns the AS3 Pool name for a Service. Includes the namespace because, once
// ResolvePartition maps every namespace onto one shared partition, a bare Service name would let
// two same-named Services in different namespaces (e.g. two apps' "frontend") write the same
// pool, with both apps' iRules routing to whichever one was merged last. Must be used by both
// ParseServices (which declares the pool) and parsePoolweight (which references it from the
// iRule). Unambiguous since neither namespaces nor Service names can contain ".".
func poolName(namespace, service string) string {
	return strings.Join([]string{"pool", namespace, service}, ".")
}

// hrParentName returns the name used to key the listener an HTTPRoute's parentRef attaches to --
// MUST agree with whatever name parseGateway used for that same listener (either gwListenerName
// for one the Gateway defines directly, or listenerSetEntryName for one contributed by an
// attached ListenerSet), or the route silently fails to attach to anything (no error, no
// crash -- just never gets routed, the exact failure mode this controller has hit before with
// sectionName handling).
//
// Takes its own fresh lock via ActiveSIGs -- safe ONLY when the caller isn't already holding
// ActiveSIGs' mutex (e.g. parser.go's parseGateway, which calls this after its own
// AttachedHTTPRoutes(gw) call has already returned and released its lock). A caller that's
// running INSIDE an already-locked SIGCache method (like _attachedHTTPRoutes, called from
// AttachedHTTPRoutes while it still holds the RLock) must use _hrParentName instead -- see its
// own doc comment for why this distinction is load-bearing, not stylistic.
func hrParentName(hr *gatewayapi.HTTPRoute, pr *gatewayapi.ParentReference) string {
	return ActiveSIGs._hrParentNameLocked(hr, pr)
}

// _hrParentNameLocked takes ActiveSIGs' own RLock once, then delegates to the cache's unlocked
// _hrParentName. Exists only so callers outside package-internal SIGCache methods (i.e. this
// free function's own external callers) get a single, obviously-locked entry point.
func (c *SIGCache) _hrParentNameLocked(hr *gatewayapi.HTTPRoute, pr *gatewayapi.ParentReference) string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c._hrParentName(hr, pr)
}

// _hrParentName is hrParentName's actual implementation, unlocked -- for calling from a context
// that already holds ActiveSIGs' mutex (see _attachedHTTPRoutes' own call site). Confirmed live
// as one half of a real deadlock: _attachedHTTPRoutes runs while AttachedHTTPRoutes' RLock is
// still held, and this used to reach back out to the LOCKING ActiveSIGs.GetListenerSet /
// GatewayRefOfListenerSet -- a second, nested RLock request from the same goroutine. Go's
// sync.RWMutex allows stacked read locks in general, but blocks any NEW reader once a writer is
// queued (to prevent writer starvation) -- and if some other goroutine's write lock request
// (e.g. GetNamespace, see its own doc comment for why that was queuing far more than it should
// have) landed in the gap between the outer RLock and this nested one, the nested RLock call
// blocks forever behind that writer, which itself can never proceed because the outer RLock it's
// waiting on is held by this same, now-stuck goroutine. A textbook circular deadlock, not a race
// -- confirmed via a live goroutine dump (/debug/pprof/goroutine?debug=2) showing exactly this
// call chain blocked on sync.RWMutex.RLock while several Set* writers queued behind it.
func (c *SIGCache) _hrParentName(hr *gatewayapi.HTTPRoute, pr *gatewayapi.ParentReference) string {
	ns := hr.Namespace
	if pr.Namespace != nil {
		ns = string(*pr.Namespace)
	}
	sn := ""
	if pr.SectionName != nil {
		sn = string(*pr.SectionName)
	}

	if pr.Kind != nil && string(*pr.Kind) == "ListenerSet" {
		// pr.Name/ns (via pr.Namespace above) refer to the ListenerSet itself here, not a
		// Gateway -- resolve it to the actual parent Gateway it's attached to, since that's
		// what listenerSetEntryName (and therefore parseGateway's own listener map) keys
		// against, not the ListenerSet's own identity in isolation.
		if ls := c.ListenerSet[utils.Keyname(ns, string(pr.Name))]; ls != nil {
			if gw := c._gatewayRefOfListenerSet(ls); gw != nil {
				return listenerSetEntryName(gw, ls, gatewayapi.SectionName(sn))
			}
		}
		// Unresolvable (ListenerSet, or its own parent Gateway, not found/cached yet) -- return
		// a name that can't collide with anything real rather than guessing at one.
		return strings.Join([]string{"gw", "unresolved-listenerset", ns, string(pr.Name), sn}, ".")
	}
	return strings.Join([]string{"gw", ns, string(pr.Name), sn}, ".")
}

// listenerSetEntryName names a specific listener contributed by a ListenerSet, once merged into
// its parent Gateway gw -- distinct from gwListenerName (used for the Gateway's own listeners)
// since a ListenerSet's listener names only need to be unique within that ListenerSet, not
// across the whole merged set (two different ListenerSets, or a ListenerSet and the Gateway
// itself, may reuse the same listener name -- see ListenerSet's own doc comment, transcribed
// from the live CRD, in third_party/gateway-api-v1.0.0-patched/apis/v1/listenerset_types.go).
func listenerSetEntryName(gw *gatewayapi.Gateway, ls *gatewayapi.ListenerSet, listenerName gatewayapi.SectionName) string {
	return strings.Join([]string{"gw", gw.Namespace, gw.Name, "ls", ls.Namespace, ls.Name, string(listenerName)}, ".")
}

// tlsServerProfileName returns the AS3 object name for a listener's TLS_Server (client-ssl)
// profile. Deliberately distinct from gwListenerName's own value: parser.go's per-listener
// host-check iRule (built whenever listener.Hostname != nil, which is every listener in
// practice) is keyed by the raw gwListenerName under "ltm/rule/", and parseSecrets used to key
// the TLS_Server profile under "ltm/profile/client-ssl/" with that SAME raw name -- two different
// Go map keys in the internal rlt map (different prefixes), but AS3 has no such prefix
// namespacing: both flatten to the identical resource name within the tenant's Application
// object, so whichever object Go's (randomized) map iteration wrote last silently won,
// and AS3 then rejected the whole tenant declaration with a 422 because the surviving object
// didn't match what the OTHER pointer (iRules or serverTLS) expected. Confirmed live via
// controller logs, not theoretical -- this broke every Virtual Server in the tenant, including
// ones unrelated to the HTTPS listener, since AS3 rejects a tenant atomically.
func tlsServerProfileName(lsname string) string {
	return lsname + ".tls-server"
}

func gwListenerName(gw *gatewayapi.Gateway, ls *gatewayapi.Listener) string {
	return strings.Join([]string{"gw", gw.Namespace, gw.Name, string(ls.Name)}, ".")
}

func tlsName(scrt *v1.Secret) string {
	return strings.Join([]string{"scrt", scrt.Namespace, scrt.Name}, ".")
}

func RouteMatches(gwNamespace string, listener *gatewayapi.Listener, routeNamespace *v1.Namespace, routeType string) bool {
	// actually, "listener" may be nil, but ".AllowedRoutes.Namespaces.From" will never be nil
	if listener == nil || listener.AllowedRoutes == nil {
		return false
	}
	namespaces := listener.AllowedRoutes.Namespaces
	if namespaces == nil || namespaces.From == nil {
		return false
	}

	matchedFrom, matchedKind := false, false

	// From
	switch *namespaces.From {
	case gatewayapi.NamespacesFromAll:
		// No need for routeNamespace here -- an "All" listener must match regardless of whether
		// the route's namespace has been cached yet. Requiring a non-nil routeNamespace before
		// this switch used to make a brand-new namespace's very first HTTPRoute (reconciled before
		// NamespaceReconciler's own informer delivered that Namespace object into the cache) match
		// nothing on every listener, even an explicit "All" one -- silently dropping the route from
		// AS3 with no error anywhere, confirmed live for kasten-io right after its namespace was
		// created.
		matchedFrom = true
	case gatewayapi.NamespacesFromSame:
		matchedFrom = routeNamespace != nil && gwNamespace == routeNamespace.Name
	case gatewayapi.NamespacesFromSelector:
		if routeNamespace == nil {
			matchedFrom = false
		} else if selector, err := metav1.LabelSelectorAsSelector(namespaces.Selector); err != nil {
			return false
		} else {
			matchedFrom = selector.Matches(labels.Set(routeNamespace.Labels))
		}
	}
	if !matchedFrom {
		return false
	}

	// Kind
	allowedKinds := listener.AllowedRoutes.Kinds
	if len(allowedKinds) == 0 {
		switch listener.Protocol {
		case gatewayapi.HTTPProtocolType:
			matchedKind = routeType == reflect.TypeOf(gatewayapi.HTTPRoute{}).Name()
		case gatewayapi.HTTPSProtocolType:
			types := []string{
				reflect.TypeOf(gatewayapi.HTTPRoute{}).Name(),
				// add other route types here.
			}
			matchedKind = utils.Contains(types, routeType)
		case gatewayapi.TLSProtocolType:
			return false
		case gatewayapi.TCPProtocolType:
			return false
		case gatewayapi.UDPProtocolType:
			return false
		}
	} else {
		for _, k := range allowedKinds {
			if k.Group != nil && *k.Group != gatewayapi.GroupName {
				return false
			} else {
				if k.Kind == gatewayapi.Kind(routeType) {
					matchedKind = true
					break
				}
			}
		}
	}

	return matchedFrom && matchedKind
}

func stringifyRGFrom(rgf *gatewayv1beta1.ReferenceGrantFrom) string {
	g := "-"
	if rgf.Group != "" {
		g = string(rgf.Group)
	}
	ns := "-"
	if rgf.Namespace != "" {
		ns = string(rgf.Namespace)
	}
	return utils.Keyname(g, string(rgf.Kind), ns)
}

func stringifyRGTo(rgt *gatewayv1beta1.ReferenceGrantTo, ns string) string {
	g := "-"
	if rgt.Group != "" {
		g = string(rgt.Group)
	}
	n := "*"
	if rgt.Name != nil {
		n = string(*rgt.Name)
	}
	return utils.Keyname(g, string(rgt.Kind), ns, n)
}

func unifiedGateways(objs []*gatewayapi.Gateway) []*gatewayapi.Gateway {

	m := map[string]bool{}
	rlt := []*gatewayapi.Gateway{}

	for _, obj := range objs {
		name := utils.Keyname(obj.Namespace, obj.Name)
		if _, f := m[name]; !f {
			m[name] = true
			rlt = append(rlt, obj)
		}
	}
	return rlt
}

func classNamesOfGateways(gws []*gatewayapi.Gateway) []string {
	rlt := []string{}

	for _, gw := range gws {
		rlt = append(rlt, string(gw.Spec.GatewayClassName))
	}

	return utils.Unified(rlt)
}

func DeployForEvent(ctx context.Context, impactedClasses []string) error {
	RequestPodARPSync()
	// slog := utils.LogFromContext(ctx)

	if len(impactedClasses) == 0 {
		return nil
	}

	ncfgs := map[string]interface{}{}
	reports := []*ClassReport{}

	for _, n := range impactedClasses {
		cfg, report, err := ParseAllForClass(n)
		if err != nil {
			return err
		}
		if report != nil {
			reports = append(reports, report)
		}
		// Partition here (GatewayClass name by default) can resolve to the same value pools do
		// below (K8s namespace by default) once ResolvePartition overrides either or both to a
		// shared configured value -- mergeTenantConfig combines them instead of one overwriting
		// the other, which plain ncfgs[n] = cfg would otherwise do silently.
		mergeTenantConfig(ncfgs, ResolvePartition(n), cfg)
	}

	scfgs, skippedServices, err := ParseClassRelatedServices(impactedClasses)
	if err != nil {
		return err
	}
	for k, cfg := range scfgs {
		cfgMap, _ := cfg.(map[string]interface{})
		// k is already a resolved partition (ParseServices -> ParseClassRelatedServices calls
		// ResolvePartition itself), not a raw namespace -- don't resolve again here.
		mergeTenantConfig(ncfgs, k, cfgMap)
	}

	as3 := RestToAS3(ncfgs)

	// Tag the request with its GatewayClasses, so AS3Deployer can report the deploy outcome
	// back as each Gateway's Programmed condition (see OnDeployFinished).
	PendingDeploys.Add(deployer.DeployRequest{
		From:    nil,
		To:      &as3,
		AS3:     true,
		Context: withDeployClasses(ctx, impactedClasses),
	})

	problems := []string{}
	for _, report := range reports {
		report.Services = skippedServices
		storeClassReport(report)
		WriteParseStatus(ctx, report)
		problems = append(problems, report.Problems()...)
	}
	// Everything that DID build is already queued above; returning an error still makes
	// controller-runtime retry this event with backoff, so a skipped object keeps getting logged
	// and is picked up on its own once fixed by something this event doesn't watch (e.g. a
	// basic-auth Secret created after the route that names it).
	if len(problems) > 0 {
		slog := utils.LogFromContext(ctx)
		for _, p := range problems {
			slog.Errorf("left out of the declaration: %s", p)
		}
		return fmt.Errorf("deployed %s without: %s", impactedClasses, strings.Join(problems, "; "))
	}
	return nil
}

// deepCopyJSON recursively copies a JSON-shaped value (the only shape AS3 declarations take,
// since they're built from and marshaled to JSON). Used to seed a new declaration from
// tenantCache without aliasing its maps/slices -- mergeTenantConfig mutates its dst in place, so
// reusing a cached map directly as that dst would corrupt the cache's own record of the last
// successfully-applied state.
func deepCopyJSON(v interface{}) interface{} {
	switch vv := v.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(vv))
		for k, val := range vv {
			out[k] = deepCopyJSON(val)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(vv))
		for i, val := range vv {
			out[i] = deepCopyJSON(val)
		}
		return out
	default:
		return vv
	}
}

// mergeTenantConfig merges src (a folder-name -> resources map, e.g. {"serviceMain": {...}}) into
// dst[partition], combining folder-level resource maps instead of overwriting when two different
// sources land on the same partition key -- which happens whenever ResolvePartition overrides
// both the Gateway/HTTPRoute side and the Service/pool side to the same configured value. Without
// this, whichever source is merged second would silently wipe out the first.
func mergeTenantConfig(dst map[string]interface{}, partition string, src map[string]interface{}) {
	if len(src) == 0 {
		return
	}
	existing, ok := dst[partition].(map[string]interface{})
	if !ok {
		dst[partition] = src
		return
	}
	for folder, resources := range src {
		resMap, ok := resources.(map[string]interface{})
		if !ok {
			existing[folder] = resources
			continue
		}
		existingFolder, ok := existing[folder].(map[string]interface{})
		if !ok {
			existing[folder] = resMap
			continue
		}
		for k, v := range resMap {
			existingFolder[k] = v
		}
	}
}

// minTenantObjectsToGuard and tenantGutFraction bound tenantWouldBeGutted's heuristic: below the
// object-count floor, a single legitimate object deletion can look like "removed everything" by
// simple arithmetic, so guarding there would block real, intended deletions of a small tenant.
// Above it, real deletions in this codebase happen one Kubernetes object at a time -- losing more
// than half of a substantial tenant's objects in one deploy is far more characteristic of a
// computation gap (some other class/event failing to contribute its share that round) than a
// deliberate bulk removal.
const (
	minTenantObjectsToGuard = 4
	tenantGutFraction       = 0.5
)

// tenantWouldBeGutted reports whether replacing a tenant's last-known-good content (oldt) with
// newly computed content (newt) would remove a suspiciously large fraction of what's currently
// deployed -- see this function's call site in AS3Deployer for the live incident (a Gateway
// listener misconfiguration silently emptying dex/argocd/everything else sharing its Virtual
// Servers) that motivated it. Counts objects per folder (e.g. "serviceMain") across both, keyed
// by folder+objectName so identically-named objects in different folders don't collide.
func tenantWouldBeGutted(oldt, newt map[string]interface{}) (gutted bool, removed int, total int) {
	oldKeys := map[string]bool{}
	for folder, res := range oldt {
		resMap, ok := res.(map[string]interface{})
		if !ok {
			continue
		}
		for k := range resMap {
			oldKeys[folder+"/"+k] = true
		}
	}
	if len(oldKeys) < minTenantObjectsToGuard {
		return false, 0, len(oldKeys)
	}

	newKeys := map[string]bool{}
	for folder, res := range newt {
		resMap, ok := res.(map[string]interface{})
		if !ok {
			continue
		}
		for k := range resMap {
			newKeys[folder+"/"+k] = true
		}
	}

	for k := range oldKeys {
		if !newKeys[k] {
			removed++
		}
	}
	return float64(removed) > float64(len(oldKeys))*tenantGutFraction, removed, len(oldKeys)
}

func (rgft *ReferenceGrantFromTo) set(rg *gatewayv1beta1.ReferenceGrant) {
	ns := rg.Namespace
	for _, f := range rg.Spec.From {
		from := stringifyRGFrom(&f)
		if _, ok := (*rgft)[from]; !ok {
			(*rgft)[from] = map[string]int8{}
		}
		for _, t := range rg.Spec.To {
			to := stringifyRGTo(&t, ns)
			(*rgft)[from][to] += 1
		}
	}
}

func (rgft *ReferenceGrantFromTo) unset(rg *gatewayv1beta1.ReferenceGrant) {
	ns := rg.Namespace
	for _, f := range rg.Spec.From {
		from := stringifyRGFrom(&f)
		if _, ok := (*rgft)[from]; !ok {
			return
		}
		for _, t := range rg.Spec.To {
			to := stringifyRGTo(&t, ns)
			if _, ok := (*rgft)[from][to]; ok {
				(*rgft)[from][to] -= 1
				if (*rgft)[from][to] == 0 {
					delete((*rgft)[from], to)
				}
			}
		}
	}
}

func (rgft *ReferenceGrantFromTo) exists(from, to string) bool {
	if toes, ok := (*rgft)[from]; !ok {
		return false
	} else {
		if v, ok := toes[to]; ok && v > 0 {
			return true
		} else {
			return false
		}
	}
}

// TODO: combine this function with that in webhooks package
func validateSecretType(group *gatewayapi.Group, kind *gatewayapi.Kind) error {
	g, k := v1.GroupName, reflect.TypeOf(v1.Secret{}).Name()
	if group != nil {
		g = string(*group)
	}
	if kind != nil {
		k = string(*kind)
	}
	if g != v1.GroupName || k != reflect.TypeOf(v1.Secret{}).Name() {
		return fmt.Errorf("not Secret type: '%s'", utils.Keyname(g, k))
	}
	return nil
}

// purgeCommonNodes tries to remove  nodes from Common if no reference.
// func purgeCommonNodes(ctx context.Context, ombs []interface{}) {
// 	for _, bp := range BIGIPs {
// 		bc := f5_bigip.BIGIPContext{Context: ctx, BIGIP: *bp}
// 		slog := utils.LogFromContext(ctx)

// 		for _, m := range ombs {
// 			partition := m.(map[string]interface{})["partition"].(string)
// 			if partition != "Common" {
// 				continue
// 			}
// 			addr := m.(map[string]interface{})["address"].(string)
// 			err := bc.Delete("ltm/node", addr, "Common", "")
// 			if err != nil && !strings.Contains(err.Error(), "is referenced by a member of pool") {
// 				slog.Warnf("cannot delete node %s: %s", addr, err.Error())
// 			}
// 		}
// 	}
// }

// // splitByPartition split the cfgs into a map of which keys are partitions
// func splitByPartition(ctx context.Context, cfgs map[string]interface{}) map[string]interface{} {
// 	partitions := map[string]map[string]map[string]interface{}{}
// 	for fstr, fv := range cfgs {
// 		for rstr, rv := range fv.(map[string]interface{}) {
// 			pstr := "unknown"
// 			if p, f := rv.(map[string]interface{})["partition"]; f {
// 				pstr = p.(string)
// 			}

// 			if _, pok := partitions[pstr]; !pok {
// 				partitions[pstr] = map[string]map[string]interface{}{}

// 			}
// 			if _, fok := partitions[pstr][fstr]; !fok {
// 				partitions[pstr][fstr] = map[string]interface{}{}
// 			}
// 			partitions[pstr][fstr][rstr] = rv
// 		}
// 	}
// 	rlt := map[string]interface{}{}
// 	for p, v := range partitions {
// 		rlt[p] = v
// 	}
// 	return rlt
// }

// filterCommonResources filter the 'Common' resources from cfgs
func filterCommonResources(cfgs map[string]interface{}) map[string]interface{} {
	rlt := map[string]interface{}{}
	for fstr, fv := range cfgs {
		if _, ok := rlt[fstr]; !ok {
			rlt[fstr] = map[string]interface{}{}
		}
		for rstr, rv := range fv.(map[string]interface{}) {
			if p, f := rv.(map[string]interface{})["partition"]; f && p == "Common" {
				rlt[fstr].(map[string]interface{})[rstr] = rv
				delete(cfgs[fstr].(map[string]interface{}), rstr)
			}
		}
	}
	return rlt
}

func NewContext() context.Context {
	reqid := uuid.New().String()
	slog := utils.NewLog().WithLevel(LogLevel).WithRequestID(reqid)
	ctxid := context.WithValue(context.TODO(), utils.CtxKey_RequestID, reqid)
	ctx := context.WithValue(ctxid, utils.CtxKey_Logger, slog)
	return ctx
}

func RestToAS3(cfgs map[string]interface{}) map[string]interface{} {

	as3 := map[string]interface{}{
		"class":   "AS3",
		"action":  "deploy",
		"persist": false,
	}

	declarations := map[string]interface{}{
		"class":         "ADC",
		"schemaVersion": "3.19.0",
	}

	for p, cfg := range cfgs {
		tenant := map[string]interface{}{
			"class": "Tenant",
		}
		for k, v := range cfg.(map[string]interface{}) {
			application := map[string]interface{}{
				"class": "Application",
			}
			for tn, resource := range v.(map[string]interface{}) {
				t, n := typeAndName(tn)
				switch t {
				case "net/arp": // skip this resource in as3 mode
				case "ltm/node":
				default:
					application[n] = resource
				}
			}
			tenant[k] = application
		}
		declarations[p] = tenant
	}

	as3["declaration"] = declarations
	return as3
}

func typeAndName(s string) (string, string) {
	a := strings.Split(s, "/")
	l := len(a)
	t := strings.Join(a[0:l-1], "/")
	n := a[l-1]
	return t, n
}

func HandleBackends(ctx context.Context, namespace string) error {
	// slog := utils.LogFromContext(ctx)
	RequestPodARPSync()

	svcs := ActiveSIGs.GetServicesWithNamespace(namespace)
	kn := []string{}
	for _, svc := range svcs {
		for _, gw := range ActiveSIGs.GetRootGateways([]*v1.Service{svc}) {
			gwc := ActiveSIGs.GetGatewayClass(string(gw.Spec.GatewayClassName))
			if gwc != nil {
				kn = append(kn, utils.Keyname(svc.Namespace, svc.Name))
				continue
			}
			// Same reasoning as ParseAllForClass's own doc comment: a GatewayClass not being
			// cached YET (startup-ordering race, no guaranteed ordering between different
			// resource kinds) is not the same as this Service's root Gateway genuinely
			// belonging to some other controller. Silently skipping it here, like this used to,
			// meant this Service could be permanently left out of this namespace's pools until
			// something else happened to re-trigger HandleBackends for it -- lower-impact than
			// the same gap in ParseAllForClass (Endpoints churn naturally retriggers this
			// function far more often than a mostly-static Gateway/HTTPRoute gets touched again),
			// but the same class of bug. Retry instead of silently omitting.
			return utils.RetryErrorf("gatewayclass %s (root of service %s/%s) not cached yet", gw.Spec.GatewayClassName, svc.Namespace, svc.Name)
		}
	}

	// No Service in this namespace is used by any of our Gateways: nothing to deploy. This used
	// to post an empty AS3 tenant named after the raw K8s namespace instead (ignoring
	// ResolvePartition) -- confirmed live: every startup declared empty "kube-system",
	// "cattle-system", "kyverno", ... tenants next to K8S-D. AS3 treats an empty tenant as
	// "delete this tenant", so a namespace that happened to share its name with any existing AS3
	// tenant on a shared BIG-IP would have wiped it. It never did any real cleanup either:
	// AS3Deployer seeds every tenant from its last-deployed state, so an empty slice merges to a
	// no-op (a removed app's pool stays behind as an orphan -- see AS3Deployer's own comment).
	if len(kn) == 0 {
		return nil
	}
	cfgs, skipped, err := ParseServices(kn)
	if err != nil {
		return err
	}
	// Not an error for this event: the rest of the namespace still deploys, and the affected
	// routes report it in their ResolvedRefs condition the next time their class is parsed.
	for svc, serr := range skipped {
		utils.LogFromContext(ctx).Errorf("service %s left out of its pool: %s", svc, serr.Error())
	}

	as3 := RestToAS3(cfgs)

	PendingDeploys.Add(deployer.DeployRequest{
		To:      &as3,
		AS3:     true,
		Context: ctx,
	})

	return nil
}

// waitForQueueToSettle blocks until lenFunc() (typically PendingDeploys.Len) hasn't changed for
// quietPeriod, polling every pollInterval, or until maxWait total has elapsed -- whichever comes
// first. See AS3Deployer's own call site for why this exists: absorbing the whole startup burst
// of reconciles into one first deploy, rather than committing tenantCache to whatever the
// earliest-arriving (and not necessarily complete) one happens to be.
func waitForQueueToSettle(lenFunc func() int, quietPeriod, pollInterval, maxWait time.Duration) {
	deadline := time.Now().Add(maxWait)
	lastLen := -1
	quietSince := time.Now()
	for time.Now().Before(deadline) {
		curLen := lenFunc()
		if curLen != lastLen {
			lastLen = curLen
			quietSince = time.Now()
		} else if time.Since(quietSince) >= quietPeriod {
			return
		}
		<-time.After(pollInterval)
	}
}

// failoverStatusIsActive reports whether bc's currently-configured URL identifies itself as the
// active member of its HA pair, via GET /mgmt/tm/cm/failover-status. Any error (unreachable,
// non-200, unexpected shape) is treated as "not confirmed active" -- ensureActiveBIGIPTarget's own
// caller needs a firm yes, not an assumption, before deciding where to send config changes.
func failoverStatusIsActive(bc *f5_bigip.BIGIPContext) bool {
	resp, err := bc.All("cm/failover-status")
	if err != nil || resp == nil {
		return false
	}
	entries, ok := (*resp)["entries"].(map[string]interface{})
	if !ok {
		return false
	}
	for _, v := range entries {
		entry, ok := v.(map[string]interface{})
		if !ok {
			continue
		}
		nested, ok := entry["nestedStats"].(map[string]interface{})
		if !ok {
			continue
		}
		nestedEntries, ok := nested["entries"].(map[string]interface{})
		if !ok {
			continue
		}
		status, ok := nestedEntries["status"].(map[string]interface{})
		if !ok {
			continue
		}
		if desc, _ := status["description"].(string); desc == "ACTIVE" {
			return true
		}
	}
	return false
}

// ensureActiveBIGIPTarget mutates bip.URL in place (via setBIGIPURL, never bip.URL= directly --
// see bigipMu's own doc comment in vars.go) to point at whichever of its own configured
// management address or cfg.Management.PeerIpAddress currently reports itself as the active HA
// member, checking fresh right before every deploy rather than caching a choice that could go
// stale the moment a real failover happens (see PeerIpAddress's own doc comment in types.go for
// the live incident this fixes: a fixed, non-floating management IP kept being targeted after it
// became the standby unit). A no-op, preserving prior behavior exactly, when PeerIpAddress isn't
// configured. Returns an error, WITHOUT changing bip.URL, if neither candidate can be confirmed
// active -- the caller must skip deploying this round rather than fall back to a guess, since
// silently posting to a guessed/wrong unit is exactly the failure mode this exists to prevent.
func ensureActiveBIGIPTarget(ctx context.Context, bip *f5_bigip.BIGIP, cfg BIGIPConfig) error {
	if cfg.Management.PeerIpAddress == nil || *cfg.Management.PeerIpAddress == "" {
		return nil
	}
	port := 443
	if cfg.Management.Port != nil {
		port = *cfg.Management.Port
	}
	peerURL := fmt.Sprintf("https://%s:%d", *cfg.Management.PeerIpAddress, port)
	originalURL := getBIGIPURL(bip)

	if failoverStatusIsActive(&f5_bigip.BIGIPContext{BIGIP: snapshotBIGIP(bip), Context: ctx}) {
		return nil
	}

	setBIGIPURL(bip, peerURL)
	if failoverStatusIsActive(&f5_bigip.BIGIPContext{BIGIP: snapshotBIGIP(bip), Context: ctx}) {
		return nil
	}

	setBIGIPURL(bip, originalURL)
	return fmt.Errorf("neither BIG-IP management address (%s, %s) confirms itself as the active "+
		"HA member -- refusing to deploy this round rather than guess which one to target",
		originalURL, peerURL)
}

// AS3 runs one declaration at a time per BIG-IP, across all partitions. With several clusters'
// controllers sharing one BIG-IP (K8S-D and K8S-T on the same pair), a POST that lands while
// another controller's deploy is running gets "503 Configuration operation in progress". Nothing
// re-sends a failed batch -- it only goes out again if some later event happens to touch the same
// tenant -- so treating that 503 as a failure leaves the change unapplied and the Gateways
// "not programmed" in the meantime. It is waited out here instead. Events arriving meanwhile pile
// up in PendingDeploys and are merged into the next batch.
var as3BusyBackoff = backoff{initial: 2 * time.Second, max: 30 * time.Second, budget: 5 * time.Minute}

type backoff struct {
	initial, max, budget time.Duration
}

// isAS3Busy matches the 503 assertBigipResp20X turns into a retry error. HandleRequest re-wraps
// it with %s, and RetryErrorf mangles its own format (it passes v, not v...), so the message reads
// "__ERROR_TO_RETRY__[503 %!!(MISSING)...". The retry mark survives, and NeedRetry finds it
// anywhere in the text; the 503 tells it apart from the 401/404/500 that are marked too.
func isAS3Busy(err error) bool {
	return utils.NeedRetry(err) && strings.Contains(err.Error(), "503")
}

// deployWhenAS3Free calls deploy until it doesn't come back with AS3 busy, or b.budget is spent.
// The wait between tries grows from b.initial to b.max and is jittered, so two controllers that
// collided once don't keep colliding in lockstep.
func deployWhenAS3Free(ctx context.Context, b backoff, deploy func() error) error {
	slog := utils.LogFromContext(ctx)
	deadline := time.Now().Add(b.budget)
	wait := b.initial
	for attempt := 1; ; attempt++ {
		err := deploy()
		if !isAS3Busy(err) {
			if err == nil && attempt > 1 {
				slog.Infof("AS3 deploy succeeded on attempt %d", attempt)
			}
			return err
		}
		sleep := wait/2 + time.Duration(rand.Int63n(int64(wait)))
		if time.Now().Add(sleep).After(deadline) {
			return fmt.Errorf("AS3 still busy after %d attempts over %s: %w", attempt, b.budget, err)
		}
		slog.Warnf("AS3 busy with another deployment (another controller on this BIG-IP?), "+
			"attempt %d, retrying in %s", attempt, sleep.Round(100*time.Millisecond))
		select {
		case <-ctx.Done():
			return err
		case <-time.After(sleep):
		}
		wait = min(2*wait, b.max)
	}
}

// AS3Deployer starts a goroutine for accepting DeployRequests and deploy them via AS3.
func AS3Deployer(stopCh chan struct{}, bigips []*f5_bigip.BIGIP) {
	tenantCache := map[string]interface{}{}
	handleNext := func() {
		// block getting from queue
		r := PendingDeploys.Get().(deployer.DeployRequest)

		as3body := *(r.To)
		slog := utils.LogFromContext(r.Context)

		// combine all requests from the queue
		l := PendingDeploys.Len()
		ids, ks := []string{}, []string{}
		declaration := map[string]interface{}{}
		// For OnDeployFinished: which GatewayClasses this batch covers, and what went wrong.
		classes := deployClassesFrom(r.Context)
		batchErrs := []error{}
		seeded := map[string]bool{}

		// Every event source here computes only ITS OWN slice of a tenant -- HandleBackends only
		// knows one namespace's Service-derived pools, DeployForEvent only knows one
		// GatewayClass's Gateway/HTTPRoute-derived virtuals+iRules. The merge loop below already
		// combines multiple requests that happen to be queued at the SAME time, but a request
		// queued ALONE (nothing else pending at that instant, the common case) had nothing to
		// merge with -- its partial view got submitted as if it were the tenant's entire desired
		// state, and AS3 "selective" mode is authoritative for whatever IS declared, so it deleted
		// every OTHER app's Virtual Server in that tenant. Confirmed live: removing one app's
		// resources (a standalone Service/Endpoints delete event, not batched with anything else)
		// wiped every other app's VIP in the same partition, leaving only the deleted app's own
		// leftover pool. Seeding from the last successfully-applied full state (tenantCache) before
		// merging this batch's own content on top fixes it: an unrelated app's resources survive
		// because they're already in the seed, not because this event happened to know about them.
		//
		// Known remaining gap: this can't distinguish "app was deleted, remove its resources" from
		// "app's namespace/class just isn't part of this particular event" -- both look like
		// "not mentioned in this batch" from here, so a deleted app's own pool/virtual can survive
		// as an orphan in AS3 until something else recomputes that same tenant with a full picture.
		// A live, incorrectly-*empty* VIP is a much worse failure mode than an inert orphaned pool
		// nothing routes to, so this trade is deliberate, not overlooked -- cleaning up the orphan
		// case properly needs the event to know it owns an authoritative slice of the tenant
		// (e.g. "all resources for this K8s namespace"), which none of these events currently track.
		seedFromCacheAndMerge := func(k string, tMap map[string]interface{}) {
			if !seeded[k] {
				seeded[k] = true
				if cached, ok := tenantCache[k].(map[string]interface{}); ok {
					declaration[k] = deepCopyJSON(cached)
				}
			}
			mergeTenantConfig(declaration, k, tMap)
		}

		processDeclaration := func(tenants map[string]interface{}) {
			for k, t := range tenants {
				if tMap, ok := t.(map[string]interface{}); ok {
					// slog.Debugf("adding tenant: %s", k)
					ks = append(ks, k)
					seedFromCacheAndMerge(k, tMap)
				} else {
					declaration[k] = t
				}
			}
		}

		processDeclaration(as3body["declaration"].(map[string]interface{}))
		ids = append(ids, utils.RequestIdFromContext(r.Context))
		for i := 0; i < l; i++ {
			m := PendingDeploys.Get().(deployer.DeployRequest)
			processDeclaration((*m.To)["declaration"].(map[string]interface{}))
			ids = append(ids, utils.RequestIdFromContext(m.Context))
			classes = append(classes, deployClassesFrom(m.Context)...)
		}
		as3body["declaration"] = declaration
		if len(ids) > 0 {
			slog.Infof("merged requests %s: tenants: %s", ids, utils.Unified(ks))
		}

		// eliminate duplicate requests
		for k, t := range as3body["declaration"].(map[string]interface{}) {
			if reflect.TypeOf(t).Kind().String() != "map" {
				continue
			}
			class, f := t.(map[string]interface{})["class"]
			if !f || class != "Tenant" {
				continue
			}

			if oldt, f := tenantCache[k]; f && utils.DeepEqual(oldt, t) {
				delete(as3body["declaration"].(map[string]interface{}), k)
			}
		}

		// Refuse to deploy a tenant whose new content would remove most of what's already
		// live, relative to tenantCache's last-known-good copy -- the earlier tenantCache-wipe
		// fix (see its own comment just above) stops a FAILURE from clearing the cache and
		// letting a later partial deploy wipe things silently, but a partial/incomplete
		// declaration can still slip through as an outright "success" on its own: AS3 accepts
		// it, nothing errors, and it still replaces the tenant's full content with just that
		// slice. This is the same failure mode surfacing with no error at all instead of a 422,
		// so it needs its own check rather than relying on errs below. Skipped (not refused)
		// for a tenant tenantCache has no prior copy of -- its first-ever deploy has nothing to
		// compare against, and refusing that would block a tenant from ever being created.
		for k, t := range as3body["declaration"].(map[string]interface{}) {
			tMap, ok := t.(map[string]interface{})
			if !ok {
				continue
			}
			if class, f := tMap["class"]; !f || class != "Tenant" {
				continue
			}
			oldt, f := tenantCache[k]
			if !f {
				continue
			}
			oldtMap, ok := oldt.(map[string]interface{})
			if !ok {
				continue
			}
			if gutted, removed, total := tenantWouldBeGutted(oldtMap, tMap); gutted {
				slog := utils.LogFromContext(r.Context)
				slog.Errorf("refusing to deploy tenant %s: this declaration would remove %d of %d "+
					"previously-deployed objects, which looks like a partial/incomplete "+
					"computation rather than an intended change -- leaving %s untouched", k, removed, total, k)
				DoneDeploys.Add(deployer.DeployResponse{
					DeployRequest: r,
					Status: fmt.Errorf("refusing to deploy tenant %s: would remove %d of %d "+
						"previously-deployed objects (likely partial/incomplete declaration)", k, removed, total),
				})
				delete(as3body["declaration"].(map[string]interface{}), k)
				batchErrs = append(batchErrs, fmt.Errorf("refused to deploy tenant %s: would remove %d of %d "+
					"previously-deployed objects", k, removed, total))
			}
		}

		// check if there's necessary to do the as3 deployment.
		found := false
		for _, t := range as3body["declaration"].(map[string]interface{}) {
			if reflect.TypeOf(t).Kind().String() != "map" {
				continue
			}
			class, f := t.(map[string]interface{})["class"]
			if !f || class != "Tenant" {
				continue
			}
			found = true
			break
		}
		// if yes
		if found {
			// debug the as3 body
			r.To = &as3body
			slog := utils.LogFromContext(r.Context)
			b, _ := json.Marshal(as3body)
			slog.Debugf("Deployed AS3: %s", string(b))

			// do as3 deployment for every BIG-IP instance.
			errs := []error{}
			for i, bip := range bigips {
				if i < len(BIPConfigs) {
					if err := ensureActiveBIGIPTarget(r.Context, bip, BIPConfigs[i]); err != nil {
						slog.Errorf(err.Error())
						DoneDeploys.Add(deployer.DeployResponse{
							DeployRequest: r,
							Status:        err,
						})
						errs = append(errs, err)
						continue
					}
				}
				err := deployWhenAS3Free(r.Context, as3BusyBackoff, func() error {
					bc := &f5_bigip.BIGIPContext{Context: r.Context, BIGIP: snapshotBIGIP(bip)}
					return deployer.HandleRequest(bc, r)
				})
				DoneDeploys.Add(deployer.DeployResponse{
					DeployRequest: r,
					Status:        err,
				})
				errs = append(errs, err)
			}

			// if deployed successfully, update tenantCache to avoid duplicate request. On failure,
			// tenantCache is left exactly as it was -- NOT cleared. Clearing it used to mean the
			// very next deploy for this tenant, however small or unrelated (a single namespace's
			// Endpoints event is a common trigger), would seedFromCacheAndMerge from nothing and
			// submit only that one event's own partial slice as if it were the tenant's entire
			// desired state. AS3 "selective" mode replaces an included tenant's full content with
			// exactly what's declared, so that partial slice doesn't just fail to add anything --
			// it WIPES every other already-deployed Virtual Server/pool/iRule in the tenant that
			// wasn't in this one small event's slice. Confirmed live: f5-private's Gateway had a
			// listener misconfiguration causing every Gateway/HTTPRoute-driven deploy for its
			// class to fail (hard error, never even reaching this deploy step) for several
			// minutes; every unrelated Service/Endpoints-driven deploy in that window kept
			// "succeeding" against a tenantCache a prior 422 had already cleared, each one
			// silently shrinking the live tenant further until dex, argocd, and everything else
			// on the same Virtual Servers were gone from BIG-IP -- with no Kubernetes object for
			// any of them ever having been deleted. Keeping the last successfully-applied state
			// cached through a failure means the WORST case is a stale-but-still-serving tenant
			// until the next fully successful comprehensive redeploy, not a cascading wipe.
			batchErrs = append(batchErrs, errs...)
			if utils.MergeErrors(errs) == nil {
				for k, t := range as3body["declaration"].(map[string]interface{}) {
					if reflect.TypeOf(t).Kind().String() != "map" {
						continue
					}
					class, f := t.(map[string]interface{})["class"]
					if !f || class != "Tenant" {
						continue
					}

					if oldt, f := tenantCache[k]; !f || !utils.DeepEqual(oldt, t) {
						tenantCache[k] = t
					}
				}
			}
		}

		// Also when nothing needed deploying (everything already matched tenantCache): that's
		// still "this configuration is on BIG-IP". Off the deployer goroutine, since status
		// writes are API round trips.
		go OnDeployFinished(classes, batchErrs)
	}

	// Startup-only: wait for the initial burst of reconciles to settle before this deployer
	// commits anything to tenantCache, rather than trusting whichever event happens to land
	// first. The moment SyncedAtStart flips true, every reconciler that was parked on it
	// (Gateway, HTTPRoute, ListenerSet, Service, Secret, ...) becomes eligible to run at once,
	// each computing and enqueueing its OWN independently-scoped contribution to shared AS3
	// partitions (many K8s namespaces' HandleBackends calls plus one or more GatewayClasses'
	// DeployForEvent calls can all target the SAME partition, e.g. "K8S-D") -- but none of them,
	// individually, IS the complete tenant. Confirmed live: a single namespace's HandleBackends
	// (5 requests) landed in PendingDeploys and got deployed/cached as if it were the entire
	// K8S-D tenant a full 5 seconds before the ~175-request comprehensive startup batch (every
	// other namespace + the f5-bigip GatewayClass's own Gateway/HTTPRoute-derived content) had
	// even finished landing in the queue. Every subsequent event was itself a normal incremental
	// update relative to that too-small seed, so nothing ever looked different enough to trigger
	// a redeploy -- the tenant stayed frozen at that one namespace's pool alone for over an hour,
	// with dex/argocd/mygateway/f5-tenants completely unreachable, until a manual pod restart.
	//
	// Debounced by watching PendingDeploys' own length rather than a fixed sleep -- a fixed
	// guess would be wrong for a much bigger or much smaller cluster; this instead waits until
	// nothing NEW has landed in the queue for a short quiet period, adapting to however long the
	// actual startup burst takes. Capped at a hard maximum so a genuinely continuously-busy
	// cluster can't delay the first real deploy forever.
	for !ActiveSIGs.SyncedAtStart {
		<-time.After(100 * time.Millisecond)
	}
	waitForQueueToSettle(PendingDeploys.Len, 2*time.Second, 500*time.Millisecond, 30*time.Second)

	for {
		select {
		case <-stopCh:
			return
		default:
			handleNext()
		}
	}
}

func RespHandler(stopCh chan struct{}) {
	handleNext := func() {
		r := DoneDeploys.Get().(deployer.DeployResponse)
		slog := utils.LogFromContext(r.Context)
		if r.Status != nil {
			slog.Errorf(r.Status.Error())
		} else {
			slog.Infof("done request handling.")
		}
	}
	for {
		select {
		case <-stopCh:
			return
		default:
			handleNext()
		}
	}
}
