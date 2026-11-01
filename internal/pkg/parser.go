package pkg

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/f5devcentral/bigip-kubernetes-gateway/internal/k8s"
	"github.com/f5devcentral/f5-bigip-rest-go/utils"
	v1 "k8s.io/api/core/v1"
	gatewayapi "sigs.k8s.io/gateway-api/apis/v1"
)

// func ParseGatewayRelatedForClass(className string, gwObjs []*gatewayapi.Gateway) (map[string]interface{}, error) {
// 	defer utils.TimeItToPrometheus()()

// 	if gwc := ActiveSIGs.GetGatewayClass(className); gwc == nil ||
// 		gwc.Spec.ControllerName != gatewayapi.GatewayController(ActiveSIGs.ControllerName) {
// 		return map[string]interface{}{}, nil
// 	}

// 	cgwObjs := []*gatewayapi.Gateway{}
// 	for _, gw := range gwObjs {
// 		if gw.Spec.GatewayClassName == gatewayapi.ObjectName(className) {
// 			cgwObjs = append(cgwObjs, gw)
// 		}
// 	}

// 	rlt := map[string]interface{}{}
// 	for _, gw := range cgwObjs {
// 		if err := parseGateway(gw, rlt); err != nil {
// 			return map[string]interface{}{}, err
// 		}
// 		hrs := ActiveSIGs.AttachedHTTPRoutes(gw)
// 		for _, hr := range hrs {
// 			if err := parseHTTPRoute(className, hr, rlt); err != nil {
// 				return map[string]interface{}{}, err
// 			}
// 		}
// 	}
// 	return map[string]interface{}{
// 		"": rlt,
// 	}, nil
// }

// ParseAllForClass builds className's virtuals, iRules, TLS profiles and certificates, plus a
// ClassReport of which Gateways and HTTPRoutes couldn't be built (nil report when the class isn't
// ours). A broken HTTPRoute or Gateway is left out of the result and recorded in the report,
// instead of failing the whole class: a single typo'd annotation or missing basic-auth Secret
// used to block every change for every app on the same GatewayClass until someone found the
// error in the logs. Only transient errors (not cached yet) still fail the whole call.
func ParseAllForClass(className string) (map[string]interface{}, *ClassReport, error) {
	defer utils.TimeItToPrometheus()()

	gwc := ActiveSIGs.GetGatewayClass(className)
	if gwc == nil {
		// Genuinely transient, not "not ours" -- see the check below for that case. The moment
		// SyncedAtStart flips true, every controller (gateway, gatewayclass, httproute,
		// listenerset, ...) that was parked on it becomes eligible to run AT ONCE, with NO
		// guaranteed ordering between different resource KINDS. A Gateway or HTTPRoute reconcile
		// can fire, and this function can run, before its OWN GatewayClass has been cached yet --
		// and since that reconcile still returns a nil error (this used to return {}, nil right
		// here, a valid "nothing to deploy" outcome as far as the caller can tell), controller-
		// runtime never automatically retries it. Confirmed live: dex/argocd/mygateway/f5-tenants
		// were all unreachable for an extended period after a fresh restart -- the AS3
		// declaration stayed frozen at just one K8s namespace's own pool, because every
		// Gateway/HTTPRoute reconcile for the "f5-bigip" class had already run (and "succeeded",
		// with nothing to contribute) before that GatewayClass was cached, and nothing else ever
		// asked them to try again. Returning a retryable error instead lets the caller's own
		// Reconcile requeue (controller-runtime retries automatically on a non-nil error, same
		// mechanism already relied on elsewhere in this codebase), same as the existing
		// SyncedAtStart gate already does for the same class of startup-ordering problem.
		return map[string]interface{}{}, nil, utils.RetryErrorf("gatewayclass %s not cached yet", className)
	}
	if gwc.Spec.ControllerName != gatewayapi.GatewayController(ActiveSIGs.ControllerName) {
		// Permanently not ours (a GatewayClass that exists and is cached, just for a different
		// controller) -- a real "nothing to do here", not a race, so no error.
		return map[string]interface{}{}, nil, nil
	}

	cgwObjs := ActiveSIGs.AttachedGateways(gwc)
	folder := "serviceMain"

	report := newClassReport(className)
	rlt := map[string]interface{}{}
	for _, gw := range cgwObjs {
		// Each Gateway is built into its own map first and only merged into rlt if it built
		// cleanly, so a failure can't leave half of a Gateway's objects in the declaration.
		grlt := map[string]interface{}{}

		// HTTPRoutes first, so parseGateway knows which route iRules actually exist before it
		// lists them on a Virtual Server -- referencing an iRule missing from the declaration
		// would make AS3 reject the whole tenant.
		unusable := map[string]bool{}
		for _, hr := range ActiveSIGs.AttachedHTTPRoutes(gw) {
			name, key := hrName(hr), utils.Keyname(hr.Namespace, hr.Name)
			if err := parseHTTPRoute(className, hr, grlt); err != nil {
				// Keep serving the route's last good iRule (from this process's lifetime) rather
				// than dropping a working app because of a bad edit.
				prev := lastGoodRouteRule(name)
				report.Routes[key] = RouteResult{Err: err, KeptPrevious: prev != nil}
				if prev != nil {
					grlt["ltm/rule/"+name] = prev
				} else {
					unusable[name] = true
				}
				continue
			}
			rememberGoodRouteRule(name, grlt["ltm/rule/"+name])
			if _, seen := report.Routes[key]; !seen {
				report.Routes[key] = RouteResult{}
			}
		}

		gwKey := utils.Keyname(gw.Namespace, gw.Name)
		if err := parseGateway(gw, grlt, unusable); err != nil {
			// Left out entirely: AS3Deployer seeds from the last deployed state, so whatever
			// this Gateway had on BIG-IP before stays as it was.
			report.Gateways[gwKey] = err
			continue
		}
		report.Gateways[gwKey] = nil
		for k, v := range grlt {
			rlt[k] = v
		}
	}
	if len(rlt) == 0 {
		return nil, report, nil
	} else {
		return map[string]interface{}{
			folder: rlt,
		}, report, nil
	}

}

// ParseRelatedServices parse all refered services
func ParseClassRelatedServices(gwc []string) (map[string]interface{}, map[string]error, error) {

	svcs := []*v1.Service{}
	for _, c := range gwc {
		gc := ActiveSIGs.GetGatewayClass(c)
		svcs = append(svcs, ActiveSIGs.RelatedServices(gc)...)
	}

	keys := []string{}
	for _, svc := range svcs {
		keys = append(keys, utils.Keyname(svc.Namespace, svc.Name))
	}
	return ParseServices(keys)
}

// ParseServices builds one Pool per Service ("namespace/name" keys). A Service that can never
// become a pool (e.g. an unsupported type) is left out and returned in skipped, keyed the same
// way, rather than failing every other pool with it; retryable errors (not cached yet) still
// fail the whole call. A pool that's left out just means its routes get no backend -- the iRule
// selects pools at runtime, so BIG-IP doesn't reject the declaration over it.
func ParseServices(svcs []string) (map[string]interface{}, map[string]error, error) {
	rlts := map[string]interface{}{}
	skipped := map[string]error{}
	for _, svc := range svcs {
		ns := strings.Split(svc, "/")[0]
		n := strings.Split(svc, "/")[1]
		// partition (the AS3 tenant / BIG-IP partition this pool lands in) is deliberately kept
		// separate from ns (the K8s namespace actually used to look up the Service/Endpoints
		// below) -- see ResolvePartition's doc comment for why these need to differ here.
		partition := ResolvePartition(ns)

		if _, f := rlts[partition]; !f {
			rlts[partition] = map[string]interface{}{
				"serviceMain": map[string]interface{}{},
			}
		}
		// name := strings.Join([]string{ns, n}, ".")
		pool := map[string]interface{}{
			"class":    "Pool",
			"monitors": []string{"tcp"},
			"members":  []interface{}{},
		}
		if fmtmbs, err := parseMembersFrom(ns, n); err != nil {
			if utils.NeedRetry(err) {
				return nil, nil, err
			}
			skipped[svc] = err
			continue
		} else {
			pool["members"] = fmtmbs
		}

		if mon, err := parseMonitorFrom(ns, n); err != nil {
			return nil, nil, err
		} else {
			pool["monitors"] = mon
		}

		// if err := parseArpsFrom(ns, n, rlt); err != nil {
		// 	return rlt, err
		// }
		// if err := parseNodesFrom(ns, n, rlt); err != nil {
		// 	return rlt, err
		// }
		rlts[partition].(map[string]interface{})["serviceMain"].(map[string]interface{})["ltm/pool/"+poolName(ns, n)] = pool
	}

	return rlts, skipped, nil
}

func parseHTTPRoute(className string, hr *gatewayapi.HTTPRoute, rlt map[string]interface{}) error {
	defer utils.TimeItToPrometheus()()

	if hr == nil {
		return nil
	}

	if err := parseiRulesFrom(className, hr, rlt); err != nil {
		return err
	}

	return nil
}

// parseGateway builds gw's Virtual Servers (and their host-filter iRules, TLS profiles and
// certificates) into rlt. unusable names HTTPRoute iRules (by hrName) that couldn't be built
// this round and must not be listed on any Virtual Server.
func parseGateway(gw *gatewayapi.Gateway, rlt map[string]interface{}, unusable map[string]bool) error {
	defer utils.TimeItToPrometheus()()

	if gw == nil {
		return nil
	}
	irules := map[string][]string{}

	// effectiveListeners answers "what listener does this vsname refer to" for the httproute-
	// attachment loop below (covers gw's own listeners AND every one contributed by an attached
	// ListenerSet -- see EffectiveListeners' own doc comment). orderedListeners is the SAME set
	// but as an ordered slice, gw's own listeners first (preserving this function's pre-
	// ListenerSet-support order/behavior exactly when nothing is attached), then each attached
	// ListenerSet's own listeners, ListenerSets ordered by creation time then namespace/name --
	// the precedence ListenerSet's own spec documents. Built separately from effectiveListeners
	// (a plain unordered map) because portGroups/portOrder below need stable ordering for
	// reproducible AS3 output between deploys, not because the two would ever disagree.
	effectiveListeners := ActiveSIGs.EffectiveListeners(gw)
	orderedListeners := []namedListener{}
	for i, listener := range gw.Spec.Listeners {
		orderedListeners = append(orderedListeners, namedListener{gwListenerName(gw, &listener), &gw.Spec.Listeners[i]})
	}
	attachedLSets := ActiveSIGs.AttachedListenerSets(gw)
	sort.Slice(attachedLSets, func(i, j int) bool {
		a, b := attachedLSets[i], attachedLSets[j]
		if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
			return a.CreationTimestamp.Before(&b.CreationTimestamp)
		}
		return utils.Keyname(a.Namespace, a.Name) < utils.Keyname(b.Namespace, b.Name)
	})
	for _, lset := range attachedLSets {
		for i, listener := range lset.Spec.Listeners {
			orderedListeners = append(orderedListeners, namedListener{listenerSetEntryName(gw, lset, listener.Name), &lset.Spec.Listeners[i]})
		}
	}

	// Group listeners sharing the same port -- once built, they'll share ONE AS3 Virtual
	// Server, since BIG-IP/AS3 rejects two different Virtual Server objects both declaring the
	// same destination IP:port ("illegally shares destination address", confirmed live
	// elsewhere in this codebase for the redirect80/auto-redirect case above). A Gateway with
	// two HTTPS listeners on the same port but different hostnames -- e.g. one wildcard plus one
	// specific, for a per-hostname certificate -- needs those merged into one Virtual Server
	// with a single TLS_Server profile carrying one certificate per hostname (matchToSNI), not
	// two separate Virtual Servers that would collide. Applies just the same whether the extra
	// listener is the Gateway's own or contributed by an attached ListenerSet.
	portOrder := []gatewayapi.PortNumber{}
	portGroups := map[gatewayapi.PortNumber][]namedListener{}
	for _, nl := range orderedListeners {
		if _, ok := portGroups[nl.listener.Port]; !ok {
			portOrder = append(portOrder, nl.listener.Port)
		}
		portGroups[nl.listener.Port] = append(portGroups[nl.listener.Port], nl)
	}

	// irules mapping: when listener.Hostname is not nil
	for _, nl := range orderedListeners {
		vsname, listener := nl.name, nl.listener
		if listener.Hostname != nil {
			if _, ok := irules[vsname]; !ok {
				irules[vsname] = []string{}
			}
			irules[vsname] = append(irules[vsname], vsname)
			rule := map[string]interface{}{
				"class": "iRule",
				"iRule": fmt.Sprintf(`
						when HTTP_REQUEST {
							if { not ([HTTP::host] matches "%s") } {
								event HTTP_REQUEST disable
							}
						}
					`, *listener.Hostname),
			}
			rlt["ltm/rule/"+vsname] = rule
		}
	}

	// irules mapping: for httproutes
	// routeNameSpecificity tracks each generated iRule's own hostname specificity (keyed by its
	// hrName), for the final flat sort below -- see that sort's own doc comment for why ordering
	// has to happen there, at individual-route granularity, rather than by reordering whole
	// listener-groups.
	routeNameSpecificity := map[string]int{}
	hrs := ActiveSIGs.AttachedHTTPRoutes(gw)
	for _, hr := range hrs {
		for _, pr := range hr.Spec.ParentRefs {
			ns := hr.Namespace
			if pr.Namespace != nil {
				ns = string(*pr.Namespace)
			}
			if pr.SectionName == nil {
				// A nil SectionName is valid per the Gateway API spec ("matches any listener",
				// not "matches none") -- _gatewayRefsOfHR already treats it that way elsewhere in
				// this codebase. Hard-erroring here used to be harmless because nothing this
				// controller created ever omitted it, but cert-manager's ACME HTTP-01 solver
				// HTTPRoute does: once ListenerSets support was enabled, its second parentRef
				// targets the ListenerSet with no SectionName. That turned this into a hard
				// failure for the WHOLE Gateway's declaration on every reconcile touching it --
				// not just skipping this one ambiguous parentRef, but aborting before anything
				// new (e.g. the solver's own iRule) ever reached AS3Deployer, while already-cached
				// routes kept appearing to work since tenantCache is left untouched on failure.
				// This HTTPRoute is still attached correctly via its OTHER, well-formed parentRef,
				// so skipping the ambiguous one is sufficient -- no need to resolve "any listener"
				// here.
				LogProblem(fmt.Sprintf("httproute %s (parent %s) sectionName", utils.Keyname(hr.Namespace, hr.Name), parentRefString(hr, &pr)),
					"parentRef has no sectionName and is ignored; the route's other parentRefs still apply")
				continue
			}
			vsname := hrParentName(hr, &pr)
			if _, ok := irules[vsname]; !ok {
				irules[vsname] = []string{}
			}
			routetype := reflect.TypeOf(*hr).Name()
			if RouteMatches(ns, effectiveListeners[vsname], ActiveSIGs.GetNamespace(hr.Namespace), routetype) {
				name := hrName(hr)
				if unusable[name] {
					continue
				}
				irules[vsname] = append(irules[vsname], name)
				// Hostname specificity only -- path length only means something when comparing
				// routes on the SAME hostname (which is what the final flat sort's stability,
				// combined with AttachedHTTPRoutes' own path-aware ordering, already preserves).
				// Mixing path length into THIS score caused an accidental cross-hostname tie:
				// kubecost's own bare "/" route scored identically to argocd's unrelated bare "/"
				// route on a completely different hostname, which (since argocd shares a listener
				// with a wildcard catch-all) put that catch-all ahead of kubecost's own ListenerSet
				// group purely by coincidence, not by actual hostname specificity.
				routeNameSpecificity[name] = hostnameSpecificity(hr.Spec.Hostnames)
			}
		}
	}

	// clientssl if exists. The second return value (which vsnames have an unresolved
	// CertificateRef, e.g. a cert-manager Certificate still being issued) doesn't need
	// consuming here -- the virtual-building loop below infers the same thing more directly, by
	// checking whether a usable TLS_Server profile actually got written to rlt for a given HTTPS
	// group.
	scrtmap, incompleteCerts, err := ActiveSIGs.AttachedSecrets(gw)
	if err != nil {
		return err
	}
	for _, nl := range orderedListeners {
		if nl.listener.Protocol != gatewayapi.HTTPSProtocolType {
			continue
		}
		key := fmt.Sprintf("gateway %s listener %s", utils.Keyname(gw.Namespace, gw.Name), nl.name)
		if incompleteCerts[nl.name] {
			LogProblem(key, "a certificateRef Secret is missing, or in another namespace without a ReferenceGrant "+
				"(cert-manager may still be issuing it)")
		} else {
			LogProblem(key, "")
		}
	}
	for _, port := range portOrder {
		group := portGroups[port]
		// Always key the TLS_Server profile by sharedListenerGroupName, even for a single
		// HTTPS listener -- see the identical reasoning on the virtual-naming loop just below for
		// why the name can't depend on the group's current size. parseSNISecrets produces the
		// exact same declaration shape parseSecrets used to for a lone default listener (no
		// hostname, or a wildcard one -- isDefaultSNIListener), since no matchToSNI ever gets
		// added for it, so this is a pure rename, not a behavior change, for every Gateway that
		// has never had more than one listener on a port.
		if len(group) > 1 {
			for _, nl := range group {
				if nl.listener.Protocol != gatewayapi.HTTPSProtocolType {
					return fmt.Errorf("listener %s: merging non-HTTPS listeners onto a shared port (%d) is not supported", nl.listener.Name, port)
				}
			}
		}
		if group[0].listener.Protocol == gatewayapi.HTTPSProtocolType {
			parseSNISecrets(tlsServerProfileName(sharedListenerGroupName(gw, port)), group, scrtmap, rlt)
		}
	}

	// virtual
	// spec.addresses is what the user requested; status.addresses is what was
	// actually assigned (by the user, via spec, or auto-assigned from
	// --address-pool-cidr -- see gateway_controller.go). Spec wins if the user
	// set it explicitly; otherwise fall back to whatever ended up in status.
	addrs := gw.Spec.Addresses
	if len(addrs) == 0 {
		for _, sa := range gw.Status.Addresses {
			addrs = append(addrs, gatewayapi.GatewayAddress{Type: sa.Type, Value: sa.Value})
		}
	}
	for i, addr := range addrs {
		if *addr.Type == gatewayapi.IPAddressType {
			ipaddr := addr.Value
			for _, port := range portOrder {
				group := portGroups[port]

				// groupName, and therefore vrname/tlsProfileName, is ALWAYS keyed by port -- not by
				// the current group size -- so it can never depend on whether a ListenerSet
				// happens to be attached and recognized at the moment a given deploy computes this.
				// It used to switch between the lone listener's own name and this shared one
				// depending on len(group), which meant an existing Gateway's virtual could rename
				// itself (single -> shared, or back) the moment a ListenerSet attach/detach was
				// (or wasn't) reflected in this particular computation. AS3 "selective" mode leaves
				// whatever's under the OLD name untouched when a declaration only mentions the NEW
				// one, so a rename doesn't replace the old Virtual Server, it leaves a second one
				// behind at the SAME destination IP:port -- which AS3 then rejects outright
				// ("illegally shares destination address..."), and since that's a per-TENANT
				// atomic failure, it silently discarded every OTHER legitimate change bundled into
				// the same deploy too. Confirmed live: repeated 422s in both naming directions
				// (single-name-vs-shared-name colliding with each other) coinciding exactly with
				// kasten's HTTPRoute/ListenerSet content never landing despite being computed
				// correctly every time.
				groupName := sharedListenerGroupName(gw, port)
				vrname := fmt.Sprintf("%s.%d", groupName, i)
				tlsProfileName := tlsServerProfileName(groupName)
				var mergedIRules []string
				if len(group) == 1 {
					mergedIRules = irules[group[0].name]
				} else {
					for _, nl := range group {
						lsname := nl.name
						for _, ir := range irules[lsname] {
							if ir == lsname {
								// This is the listener's own host-disable iRule (self-named,
								// appended first -- see the "irules mapping: when
								// listener.Hostname is not nil" block above). It must NOT be
								// carried onto a shared Virtual Server: `event HTTP_REQUEST
								// disable` affects the WHOLE iRule chain for this request, not
								// just the iRule that called it, so a listener whose hostname
								// DOESN'T match would wrongly kill a different, already-matched
								// listener's HTTPRoute iRules running later in the same merged
								// chain. Each HTTPRoute's own generated iRule already re-checks
								// its own hostname internally (irule_templates/irule.tmpl) and
								// is therefore safe to merge as-is.
								continue
							}
							mergedIRules = append(mergedIRules, ir)
						}
					}

					// Final sort by each iRule's own hostname specificity (exact > wildcard >
					// none), stable so routes tied on hostname (almost always: multiple routes on
					// the SAME host) keep whatever relative order they already had -- which is
					// where path-based precedence (AttachedHTTPRoutes' own sort) actually lives,
					// and the merge loop's own order (just above) is irrelevant to it either way.
					// This has to happen at this flat, per-route granularity rather than by
					// reordering whole listener-groups: a single listener can host a MIX of
					// specificities (e.g. f5-private's own "https" section carrying both argocd's
					// specific route and a wildcard-hostname catch-all together), which no
					// group-level ordering can express correctly. Confirmed live: an earlier
					// group-level-only version of this fix left a wildcard catch-all ranked ahead
					// of a real app's own dedicated ListenerSet, because the catch-all's listener
					// group also happened to contain an unrelated app with a coincidentally-equal
					// score. Only done for len(group) > 1 -- the len==1 case's mergedIRules still
					// carries its listener's own self-named host-disable iRule first (excluded
					// above, but only for the multi-listener merge), which this sort has no entry
					// for in routeNameSpecificity and would wrongly displace from that position.
					sort.SliceStable(mergedIRules, func(i, j int) bool {
						return routeNameSpecificity[mergedIRules[i]] > routeNameSpecificity[mergedIRules[j]]
					})
				}

				// All listeners in a multi-listener group are validated HTTPS-only above (in
				// the clientssl section) -- a single-listener group keeps its own protocol.
				protocol := group[0].listener.Protocol
				if protocol == gatewayapi.HTTPSProtocolType {
					if _, ok := rlt["ltm/profile/client-ssl/"+tlsProfileName]; !ok {
						// No listener in this group ended up with a resolved cert (see
						// AttachedSecrets' own doc comment -- most commonly a cert-manager
						// Certificate still being issued, not a permanent error). Building this
						// Virtual Server anyway would point serverTLS at a TLS_Server profile
						// that doesn't exist in this same declaration, which AS3 would reject.
						// Skip just this group; everything else in the Gateway/class still
						// deploys normally, and this group picks back up on its own once the
						// cert resolves and something triggers a redeploy (e.g. the Secret
						// appearing at all triggers this controller's own secret_controller.go).
						LogProblem(fmt.Sprintf("gateway %s port %d", utils.Keyname(gw.Namespace, gw.Name), port),
							"HTTPS Virtual Server not created: no listener on this port has a usable certificate")
						continue
					}
					LogProblem(fmt.Sprintf("gateway %s port %d", utils.Keyname(gw.Namespace, gw.Name), port), "")
				}

				virtual := map[string]interface{}{}
				switch protocol {
				case gatewayapi.HTTPProtocolType:
					virtual["profileHTTP"] = "basic"
					virtual["class"] = "Service_HTTP"
				case gatewayapi.HTTPSProtocolType:
					virtual["class"] = "Service_HTTPS"
					virtual["profileHTTP"] = "basic"
					virtual["serverTLS"] = tlsProfileName
					// AS3's Service_HTTPS defaults redirect80 to true, auto-creating an implicit
					// "<name>-Redirect-" Virtual Server on port 80 for the same destination IP.
					// Gateway API listeners are independent objects with their own explicit HTTP
					// listener + iRule-based redirect (built above, in the listener.Hostname !=
					// nil block) -- when both an HTTP and HTTPS listener exist on one Gateway
					// (the normal case), AS3's auto-generated redirect collides with our own
					// explicit HTTP Virtual Server at the exact same IP:port:protocol:vlan and
					// rejects the ENTIRE tenant declaration. Confirmed live via the actual AS3
					// error ("illegally shares destination address... with ...-Redirect-"), not
					// theoretical -- this was silently wiping out every Virtual Server in the
					// tenant (including unrelated ones sharing the same partition), not just this
					// Gateway's own.
					virtual["redirect80"] = false
				case gatewayapi.TCPProtocolType:
					return fmt.Errorf("unsupported ProtocolType: %s", protocol)
				case gatewayapi.UDPProtocolType:
					return fmt.Errorf("unsupported ProtocolType: %s", protocol)
				case gatewayapi.TLSProtocolType:
					return fmt.Errorf("unsupported ProtocolType: %s", protocol)
				}

				virtual["virtualAddresses"] = []string{ipaddr}
				virtual["virtualPort"] = port
				virtual["iRules"] = mergedIRules
				if addr := ResolveBackendSnatAddress(); addr != "" {
					// AS3-managed SNAT_Pool, referenced by AS3 "use" pointer (sibling object name
					// within serviceMain) -- not a {"bigip": "/partition/name"} pointer to a
					// manually tmsh-created object. The latter was tried first and repeatedly
					// vanished from BIG-IP between deploys (confirmed live, twice): AS3's
					// "selective" tenant sync doesn't reliably leave non-AS3-owned objects of this
					// type alone. Declaring it here means AS3 recreates it on every apply, the same
					// self-healing guarantee pools/virtuals already have.
					rlt["ltm/snatpool/backend-snat"] = map[string]interface{}{
						"class":         "SNAT_Pool",
						"snatAddresses": []string{addr},
					}
					virtual["snat"] = map[string]interface{}{"use": "backend-snat"}
				} else {
					virtual["snat"] = "auto"
				}
				rlt["ltm/virtual/"+vrname] = virtual
			}
		} else {
			return fmt.Errorf("unsupported AddressType: %s", *addr.Type)
		}
	}

	return nil
}

// TODO: find the way to set monitor
func parseMonitorFrom(svcNamespace, svcName string) (interface{}, error) {
	return []string{"tcp"}, nil
}

// wantedServicePortNames returns the set of svc's own port Names that are actually referenced by
// an HTTPRoute backendRef pointing at it (backendRef.Port is the Service's port NUMBER, resolved
// here to that port's Name so it can be matched against Endpoints subset ports -- see
// FormatMembersFromServiceEndpoints). Empty means no backendRef anywhere specified an explicit
// port for this Service, so the caller should fall back to including every port unfiltered.
func wantedServicePortNames(svc *v1.Service) map[string]bool {
	names := map[string]bool{}
	for _, hr := range ActiveSIGs.HTTPRoutesRefsOf(svc) {
		for _, rl := range hr.Spec.Rules {
			for _, br := range rl.BackendRefs {
				ns := hr.Namespace
				if br.Namespace != nil {
					ns = string(*br.Namespace)
				}
				if utils.Keyname(ns, string(br.Name)) != utils.Keyname(svc.Namespace, svc.Name) || br.Port == nil {
					continue
				}
				for _, p := range svc.Spec.Ports {
					if p.Port == int32(*br.Port) {
						names[p.Name] = true
					}
				}
			}
		}
	}
	return names
}

func parseMembersFrom(svcNamespace, svcName string) ([]interface{}, error) {
	keyname := utils.Keyname(svcNamespace, svcName)
	svc := ActiveSIGs.GetService(keyname)
	eps := ActiveSIGs.GetEndpoints(keyname)
	// A cache miss here used to silently produce a zero-member Pool instead of an error --
	// callers only ever pass in a (namespace, name) pair they already believe is relevant (a
	// namespace's own Service list, or one referenced by an HTTPRoute's backendRef), so a miss
	// means either this controller's own Service/Endpoints cache genuinely hasn't caught up yet
	// (the same class of startup/ordering race fixed elsewhere in this codebase, e.g.
	// ParseAllForClass's own "gatewayclass not cached yet" handling) or the referenced Service
	// really doesn't exist. Both cases deserve a real, visible error and a retry (which
	// DeployForEvent/HandleBackends' own error propagation now aborts the deploy for, rather than
	// silently pushing an empty pool to BIG-IP and taking the service down) -- neither deserves to
	// look, from BIG-IP's perspective, identical to "this Service legitimately has zero ready
	// backends right now" (svc and eps both present, eps.Subsets just empty), which is NOT an
	// error and must still produce an empty-but-declared Pool.
	if svc == nil {
		return nil, utils.RetryErrorf("service %s not cached yet", keyname)
	}
	if eps == nil {
		return nil, utils.RetryErrorf("endpoints for service %s not cached yet", keyname)
	}
	if mbs, err := k8s.FormatMembersFromServiceEndpoints(svc, eps, wantedServicePortNames(svc)); err != nil {
		return []interface{}{}, err
	} else {
		fmtmbs := []interface{}{}

		for _, mb := range mbs {
			fmtmbs = append(fmtmbs, map[string]interface{}{
				"servicePort":     mb.TargetPort,
				"serverAddresses": []string{mb.IpAddr},
			})
		}
		return fmtmbs, nil
	}
}

// func parseArpsFrom(svcNamespace, svcName string, rlt map[string]interface{}) error {
// 	svc := ActiveSIGs.GetService(utils.Keyname(svcNamespace, svcName))
// 	eps := ActiveSIGs.GetEndpoints(utils.Keyname(svcNamespace, svcName))
// 	if svc != nil && eps != nil {
// 		if mbs, err := k8s.FormatMembersFromServiceEndpoints(svc, eps); err != nil {
// 			return err
// 		} else {
// 			prefix := "k8s-"
// 			for _, mb := range mbs {
// 				if mb.MacAddr != "" {
// 					rlt["net/arp/"+prefix+mb.IpAddr] = map[string]interface{}{
// 						"name":       prefix + mb.IpAddr,
// 						"ipAddress":  mb.IpAddr,
// 						"macAddress": mb.MacAddr,
// 					}
// 				}
// 			}
// 		}
// 	}
// 	return nil
// }

// func parseNodesFrom(svcNamespace, svcName string, rlt map[string]interface{}) error {
// 	svc := ActiveSIGs.GetService(utils.Keyname(svcNamespace, svcName))
// 	eps := ActiveSIGs.GetEndpoints(utils.Keyname(svcNamespace, svcName))
// 	if svc != nil && eps != nil {
// 		if mbs, err := k8s.FormatMembersFromServiceEndpoints(svc, eps); err != nil {
// 			return err
// 		} else {
// 			for _, mb := range mbs {
// 				rlt["ltm/node/"+mb.IpAddr] = map[string]interface{}{
// 					"name":    mb.IpAddr,
// 					"address": mb.IpAddr,
// 					"monitor": "default",
// 					"session": "user-enabled",
// 				}
// 			}
// 		}
// 	}
// 	return nil
// }

// namedListener pairs a listener with the vsname it was keyed by -- gwListenerName for one the
// Gateway declares directly, listenerSetEntryName for one contributed by an attached
// ListenerSet. Needed wherever code has to look a listener's own iRules/certs back up in a map
// keyed that way (irules, scrtmap) without re-deriving the name (which requires knowing whether
// the listener came from the Gateway or a specific ListenerSet -- information the bare
// *gatewayapi.Listener alone doesn't carry).
type namedListener struct {
	name     string
	listener *gatewayapi.Listener
}

// sharedListenerGroupName names the merged Virtual Server / TLS_Server profile for a group of
// listeners that share one port (see the grouping comment in parseGateway). Distinct from any
// real gwListenerName output as long as no listener is literally named "shared-<port>".
func sharedListenerGroupName(gw *gatewayapi.Gateway, port gatewayapi.PortNumber) string {
	return strings.Join([]string{"gw", gw.Namespace, gw.Name, fmt.Sprintf("shared-%d", port)}, ".")
}

// isDefaultSNIListener reports whether ls should be the SNI-default certificate (the one used
// when a client's SNI doesn't match any other entry, or presents none) in a merged TLS_Server
// profile -- a listener with no hostname, or a wildcard ("*.") one, both being the broadest
// match available.
func isDefaultSNIListener(ls *gatewayapi.Listener) bool {
	return ls.Hostname == nil || strings.HasPrefix(string(*ls.Hostname), "*.")
}

// parseSNISecrets builds ONE TLS_Server profile shared by multiple listeners merged onto the
// same Virtual Server (see parseGateway's grouping comment for why), selecting between their
// certificates by SNI -- AS3's mechanism for exactly this: each certificate entry may carry a
// matchToSNI value, and the one entry with it omitted is the default (used when SNI is absent or
// matches nothing else). isDefaultSNIListener decides which listener's cert(s) go in without
// matchToSNI; if more than one listener qualifies as default, the last one encountered wins --
// an ambiguous config this doesn't try to validate.
func parseSNISecrets(profileName string, group []namedListener, scrtmap map[string][]*v1.Secret, rlt map[string]interface{}) {
	certs := []interface{}{}
	for _, nl := range group {
		lsname, ls := nl.name, nl.listener
		isDefault := isDefaultSNIListener(ls)
		for _, scrt := range scrtmap[lsname] {
			name := tlsName(scrt)
			rlt["sys/file/certificate/"+name] = map[string]interface{}{
				"class":       "Certificate",
				"certificate": string(scrt.Data[v1.TLSCertKey]),
				"privateKey":  string(scrt.Data[v1.TLSPrivateKeyKey]),
			}
			entry := map[string]interface{}{"certificate": name}
			if !isDefault {
				entry["matchToSNI"] = string(*ls.Hostname)
			}
			certs = append(certs, entry)
		}
	}
	if len(certs) > 0 {
		rlt["ltm/profile/client-ssl/"+profileName] = map[string]interface{}{
			"class":        "TLS_Server",
			"certificates": certs,
		}
	}
}
