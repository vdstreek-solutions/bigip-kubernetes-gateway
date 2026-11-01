package pkg

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/f5devcentral/f5-bigip-rest-go/utils"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayapi "sigs.k8s.io/gateway-api/apis/v1"
)

// StatusClient writes Gateway API status (set from cmd/controller to the manager's client).
// Nil disables status writing entirely, e.g. in unit tests.
var StatusClient client.Client

// ClassReport records what the last ParseAllForClass of one GatewayClass couldn't build, per
// object. It drives the Gateway API status this controller writes, and lets the Programmed
// condition (only known once AS3Deployer has actually deployed) tell a Gateway that was left out
// of the declaration apart from one whose deploy failed.
type ClassReport struct {
	Class    string
	Gateways map[string]error       // "namespace/name" -> nil if built, else why it was left out
	Routes   map[string]RouteResult // "namespace/name" of each attached HTTPRoute
	Services map[string]error       // "namespace/name" -> why that Service couldn't become a pool
}

type RouteResult struct {
	Err          error
	KeptPrevious bool // Err != nil, but its last good iRule is still in the declaration
}

func newClassReport(className string) *ClassReport {
	return &ClassReport{
		Class:    className,
		Gateways: map[string]error{},
		Routes:   map[string]RouteResult{},
		Services: map[string]error{},
	}
}

// Problems lists everything that was left out of (or frozen in) this class's declaration.
func (r *ClassReport) Problems() []string {
	problems := []string{}
	for k, err := range r.Gateways {
		if err != nil {
			problems = append(problems, fmt.Sprintf("gateway %s (%s)", k, err.Error()))
		}
	}
	for k, res := range r.Routes {
		if res.Err != nil {
			problems = append(problems, fmt.Sprintf("httproute %s (%s)", k, res.Err.Error()))
		}
	}
	return problems
}

var (
	classReportsMu sync.Mutex
	classReports   = map[string]*ClassReport{}

	lastGoodRulesMu sync.Mutex
	lastGoodRules   = map[string]interface{}{} // hrName -> last iRule object that built cleanly
)

func storeClassReport(r *ClassReport) {
	classReportsMu.Lock()
	defer classReportsMu.Unlock()
	classReports[r.Class] = r
}

func loadClassReport(className string) *ClassReport {
	classReportsMu.Lock()
	defer classReportsMu.Unlock()
	return classReports[className]
}

func lastGoodRouteRule(name string) interface{} {
	lastGoodRulesMu.Lock()
	defer lastGoodRulesMu.Unlock()
	return lastGoodRules[name]
}

func rememberGoodRouteRule(name string, rule interface{}) {
	lastGoodRulesMu.Lock()
	defer lastGoodRulesMu.Unlock()
	lastGoodRules[name] = rule
}

// ForgetHTTPRoute drops a deleted HTTPRoute's last good iRule, so a later route with the same
// namespace/name that fails to build doesn't come back serving the old one.
func ForgetHTTPRoute(namespace, name string) {
	lastGoodRulesMu.Lock()
	defer lastGoodRulesMu.Unlock()
	delete(lastGoodRules, strings.Join([]string{"hr", namespace, name}, "."))
}

var (
	loggedProblemsMu sync.Mutex
	loggedProblems   = map[string]string{} // problem key -> message last logged
)

// LogProblem puts a configuration problem in the controller log: as a warning the first time
// it's seen in this process (and again whenever its message changes), and as one info line once
// it clears (message ""). Status shows the same problems, but status writes can fail and a fresh
// pod finds the old status unchanged -- the log must not depend on either. Deduplicated because
// the same problem is re-found on every reconcile of its GatewayClass.
func LogProblem(key, message string) {
	loggedProblemsMu.Lock()
	defer loggedProblemsMu.Unlock()
	prev, seen := loggedProblems[key]
	slog := utils.LogFromContext(context.TODO())
	switch {
	case message == "" && seen:
		delete(loggedProblems, key)
		slog.Infof("%s: resolved", key)
	case message != "" && message != prev:
		loggedProblems[key] = message
		slog.Warnf("%s: %s", key, message)
	}
}

type deployClassesKey struct{}

func withDeployClasses(ctx context.Context, classes []string) context.Context {
	return context.WithValue(ctx, deployClassesKey{}, classes)
}

func deployClassesFrom(ctx context.Context) []string {
	if ctx == nil {
		return nil
	}
	classes, _ := ctx.Value(deployClassesKey{}).([]string)
	return classes
}

// WriteParseStatus writes everything known right after parsing: Accepted on each Gateway and
// ListenerSet, a Programmed=False for Gateways that weren't (or can't be) deployed at all, and
// Accepted/ResolvedRefs for each HTTPRoute parentRef that targets this class. Programmed for the
// rest follows from OnDeployFinished once AS3Deployer is done.
func WriteParseStatus(ctx context.Context, report *ClassReport) {
	if report == nil {
		return
	}
	gwc := ActiveSIGs.GetGatewayClass(report.Class)
	if gwc == nil {
		return
	}
	for _, gw := range ActiveSIGs.AttachedGateways(gwc) {
		gwKey := utils.Keyname(gw.Namespace, gw.Name)
		gerr, parsed := report.Gateways[gwKey]
		if !parsed {
			continue
		}
		hasAddress := gatewayHasAddress(gw)
		switch {
		case gerr != nil:
			// Already logged as an error by DeployForEvent; this tracks it so its fix gets logged.
			LogProblem("gateway "+gwKey, "not deployed: "+gerr.Error())
		case !hasAddress:
			LogProblem("gateway "+gwKey, "no address in spec.addresses or status.addresses, so no Virtual Server is created")
		default:
			LogProblem("gateway "+gwKey, "")
		}
		updateGatewayConditions(ctx, gw.Namespace, gw.Name, func(gen int64) []metav1.Condition {
			if gerr != nil {
				return []metav1.Condition{
					condition(gatewayapi.GatewayConditionAccepted, false, gatewayapi.GatewayReasonListenersNotValid, gerr.Error(), gen),
					condition(gatewayapi.GatewayConditionProgrammed, false, gatewayapi.GatewayReasonInvalid,
						"not deployed; BIG-IP keeps this Gateway's previous configuration: "+gerr.Error(), gen),
				}
			}
			conds := []metav1.Condition{
				condition(gatewayapi.GatewayConditionAccepted, true, gatewayapi.GatewayReasonAccepted, "Accepted by "+ActiveSIGs.ControllerName, gen),
			}
			if !hasAddress {
				conds = append(conds, condition(gatewayapi.GatewayConditionProgrammed, false, gatewayapi.GatewayReasonAddressNotAssigned,
					"no address in spec.addresses or status.addresses, so no Virtual Server is created", gen))
			}
			return conds
		})

		attached := map[string]bool{}
		for _, ls := range ActiveSIGs.AttachedListenerSets(gw) {
			attached[utils.Keyname(ls.Namespace, ls.Name)] = true
		}
		for _, ls := range ActiveSIGs.ListenerSetsReferencingGateway(gw) {
			ok := attached[utils.Keyname(ls.Namespace, ls.Name)]
			if ok {
				LogProblem("listenerset "+utils.Keyname(ls.Namespace, ls.Name), "")
			} else {
				LogProblem("listenerset "+utils.Keyname(ls.Namespace, ls.Name),
					fmt.Sprintf("not attached: gateway %s does not allow it (spec.allowedListeners)", gwKey))
			}
			updateListenerSetConditions(ctx, ls.Namespace, ls.Name, func(gen int64) []metav1.Condition {
				if !ok {
					msg := fmt.Sprintf("gateway %s does not allow this ListenerSet (spec.allowedListeners)", gwKey)
					return []metav1.Condition{
						condition("Accepted", false, "NotAllowed", msg, gen),
						condition("Programmed", false, "NotAllowed", msg, gen),
					}
				}
				conds := []metav1.Condition{condition("Accepted", true, "Accepted", "Accepted by "+ActiveSIGs.ControllerName, gen)}
				if gerr != nil {
					conds = append(conds, condition("Programmed", false, "Invalid", "parent gateway "+gwKey+" was not deployed: "+gerr.Error(), gen))
				}
				return conds
			})
		}
	}

	for _, hr := range ActiveSIGs.HTTPRoutesReferencingClass(report.Class) {
		logRouteProblems(hr, report)
		writeRouteStatus(ctx, hr, report)
	}
}

// logRouteProblems logs, per parentRef into report's class, the same Accepted/ResolvedRefs
// outcome writeRouteStatus writes -- from the cache, so it doesn't depend on status writes.
func logRouteProblems(hr *gatewayapi.HTTPRoute, report *ClassReport) {
	hrKey := utils.Keyname(hr.Namespace, hr.Name)
	result := report.Routes[hrKey]
	resolved := routeResolvedRefs(hr, report)(hr.Generation)
	for i := range hr.Spec.ParentRefs {
		gw, attached := ActiveSIGs.ParentRefTarget(hr, &hr.Spec.ParentRefs[i])
		if gw == nil || string(gw.Spec.GatewayClassName) != report.Class {
			continue
		}
		key := fmt.Sprintf("httproute %s (parent %s)", hrKey, parentRefString(hr, &hr.Spec.ParentRefs[i]))
		switch {
		case !attached:
			LogProblem(key, fmt.Sprintf("not attached: no listener on gateway %s accepts it (check sectionName and allowedRoutes)",
				utils.Keyname(gw.Namespace, gw.Name)))
		case result.Err != nil:
			// Already logged as an error by DeployForEvent; this tracks it so its fix gets logged.
			LogProblem(key, "not deployed: "+result.Err.Error())
		case resolved.Status != metav1.ConditionTrue:
			LogProblem(key, "unresolved backends: "+resolved.Message)
		default:
			LogProblem(key, "")
		}
	}
}

func parentRefString(hr *gatewayapi.HTTPRoute, pr *gatewayapi.ParentReference) string {
	ns := hr.Namespace
	if pr.Namespace != nil {
		ns = string(*pr.Namespace)
	}
	s := utils.Keyname(ns, string(pr.Name))
	if pr.Kind != nil && *pr.Kind != "Gateway" {
		s = string(*pr.Kind) + " " + s
	}
	if pr.SectionName != nil {
		s += "/" + string(*pr.SectionName)
	}
	return s
}

// OnDeployFinished sets Programmed on every Gateway (and attached ListenerSet) of classes from
// one AS3Deployer batch. errs are that batch's failures; none means everything deployed (or was
// already deployed). Gateways WriteParseStatus already marked as not deployable are left alone.
func OnDeployFinished(classes []string, errs []error) {
	msgs := []string{}
	for _, err := range errs {
		if err != nil {
			msgs = append(msgs, err.Error())
		}
	}
	ctx := context.Background()
	for _, className := range utils.Unified(classes) {
		gwc := ActiveSIGs.GetGatewayClass(className)
		report := loadClassReport(className)
		if gwc == nil || report == nil || gwc.Spec.ControllerName != gatewayapi.GatewayController(ActiveSIGs.ControllerName) {
			continue
		}
		for _, gw := range ActiveSIGs.AttachedGateways(gwc) {
			gwKey := utils.Keyname(gw.Namespace, gw.Name)
			if gerr, parsed := report.Gateways[gwKey]; !parsed || gerr != nil || !gatewayHasAddress(gw) {
				continue
			}
			// The AS3 error itself is already logged by RespHandler; this records which Gateways
			// it left unprogrammed, and logs when they recover.
			if len(msgs) > 0 {
				LogProblem("gateway "+gwKey+" (deploy)", "not programmed: AS3 deploy failed")
			} else {
				LogProblem("gateway "+gwKey+" (deploy)", "")
			}
			programmed := func(gen int64) metav1.Condition {
				if len(msgs) > 0 {
					return condition(gatewayapi.GatewayConditionProgrammed, false, gatewayapi.GatewayReasonInvalid,
						"AS3 deploy failed: "+strings.Join(msgs, "; "), gen)
				}
				return condition(gatewayapi.GatewayConditionProgrammed, true, gatewayapi.GatewayReasonProgrammed, "Deployed to BIG-IP", gen)
			}
			updateGatewayConditions(ctx, gw.Namespace, gw.Name, func(gen int64) []metav1.Condition {
				return []metav1.Condition{programmed(gen)}
			})
			for _, ls := range ActiveSIGs.AttachedListenerSets(gw) {
				updateListenerSetConditions(ctx, ls.Namespace, ls.Name, func(gen int64) []metav1.Condition {
					return []metav1.Condition{programmed(gen)}
				})
			}
		}
	}
}

func writeRouteStatus(ctx context.Context, hr *gatewayapi.HTTPRoute, report *ClassReport) {
	if StatusClient == nil {
		return
	}
	key := types.NamespacedName{Namespace: hr.Namespace, Name: hr.Name}
	result := report.Routes[key.String()]

	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		var latest gatewayapi.HTTPRoute
		if err := StatusClient.Get(wctx, key, &latest); err != nil {
			return client.IgnoreNotFound(err)
		}

		ours := gatewayapi.GatewayController(ActiveSIGs.ControllerName)
		targetsThisClass := func(pr *gatewayapi.ParentReference) (*gatewayapi.Gateway, bool, bool) {
			gw, attached := ActiveSIGs.ParentRefTarget(&latest, pr)
			return gw, attached, gw != nil && string(gw.Spec.GatewayClassName) == report.Class
		}
		inSpec := func(pr gatewayapi.ParentReference) bool {
			for _, spr := range latest.Spec.ParentRefs {
				if reflect.DeepEqual(spr, pr) {
					return true
				}
			}
			return false
		}

		// Keep other controllers' entries, and ours for parentRefs into another of our classes;
		// rebuild ours for this class; drop ours for parentRefs no longer in the spec.
		parents := []gatewayapi.RouteParentStatus{}
		previous := map[int]gatewayapi.RouteParentStatus{}
		for _, ps := range latest.Status.Parents {
			if ps.ControllerName != ours {
				parents = append(parents, ps)
				continue
			}
			if !inSpec(ps.ParentRef) {
				continue
			}
			if _, _, thisClass := targetsThisClass(&ps.ParentRef); !thisClass {
				parents = append(parents, ps)
				continue
			}
			for i, spr := range latest.Spec.ParentRefs {
				if reflect.DeepEqual(spr, ps.ParentRef) {
					previous[i] = ps
				}
			}
		}

		resolved := routeResolvedRefs(&latest, report)
		for i := range latest.Spec.ParentRefs {
			pr := latest.Spec.ParentRefs[i]
			gw, attached, thisClass := targetsThisClass(&pr)
			if !thisClass {
				continue
			}
			gen := latest.Generation
			var accepted metav1.Condition
			switch {
			case !attached:
				accepted = condition(gatewayapi.RouteConditionAccepted, false, gatewayapi.RouteReasonNotAllowedByListeners,
					fmt.Sprintf("no listener on gateway %s accepts this route (check sectionName and allowedRoutes)",
						utils.Keyname(gw.Namespace, gw.Name)), gen)
			case result.Err != nil:
				msg := result.Err.Error()
				if result.KeptPrevious {
					msg += " -- BIG-IP keeps serving the previous version of this route"
				}
				accepted = condition(gatewayapi.RouteConditionAccepted, false, gatewayapi.RouteReasonUnsupportedValue, msg, gen)
			default:
				accepted = condition(gatewayapi.RouteConditionAccepted, true, gatewayapi.RouteReasonAccepted, "Accepted by "+ActiveSIGs.ControllerName, gen)
			}

			ps := previous[i]
			ps.ParentRef = pr
			ps.ControllerName = ours
			meta.SetStatusCondition(&ps.Conditions, accepted)
			meta.SetStatusCondition(&ps.Conditions, resolved(gen))
			parents = append(parents, ps)
		}

		if parentStatusesEqual(latest.Status.Parents, parents) {
			return nil
		}
		latest.Status.Parents = parents
		return StatusClient.Status().Update(wctx, &latest)
	})
	if err != nil {
		utils.LogFromContext(ctx).Warnf("failed to update status of httproute %s: %s", key, err.Error())
	}
}

// routeResolvedRefs checks every backendRef of hr the way the parser resolves it.
func routeResolvedRefs(hr *gatewayapi.HTTPRoute, report *ClassReport) func(gen int64) metav1.Condition {
	reason, problems := gatewayapi.RouteReasonResolvedRefs, []string{}
	for _, rl := range hr.Spec.Rules {
		for _, br := range rl.BackendRefs {
			ns := hr.Namespace
			if br.Namespace != nil {
				ns = string(*br.Namespace)
			}
			svcKey := utils.Keyname(ns, string(br.Name))
			if (br.Group != nil && *br.Group != "") || (br.Kind != nil && *br.Kind != "Service") {
				reason = gatewayapi.RouteReasonInvalidKind
				problems = append(problems, fmt.Sprintf("backendRef %s is not a core Service", svcKey))
				continue
			}
			svc := ActiveSIGs.GetService(svcKey)
			switch {
			case svc == nil:
				reason = gatewayapi.RouteReasonBackendNotFound
				problems = append(problems, fmt.Sprintf("service %s not found", svcKey))
			case !ActiveSIGs.CanRefer(hr, svc):
				reason = gatewayapi.RouteReasonRefNotPermitted
				problems = append(problems, fmt.Sprintf("service %s is in another namespace and no ReferenceGrant allows it", svcKey))
			case report.Services[svcKey] != nil:
				reason = gatewayapi.RouteReasonBackendNotFound
				problems = append(problems, fmt.Sprintf("service %s can't be used as a pool: %s", svcKey, report.Services[svcKey].Error()))
			}
		}
	}
	return func(gen int64) metav1.Condition {
		if len(problems) == 0 {
			return condition(gatewayapi.RouteConditionResolvedRefs, true, reason, "All references resolved", gen)
		}
		return condition(gatewayapi.RouteConditionResolvedRefs, false, reason, strings.Join(problems, "; "), gen)
	}
}

// updateGatewayConditions sets the conditions build returns on a Gateway (by type, leaving any
// others untouched) and writes status only if something actually changed -- status writes
// trigger this controller's own Gateway reconcile, so an unconditional write would loop.
func updateGatewayConditions(ctx context.Context, namespace, name string, build func(gen int64) []metav1.Condition) {
	if StatusClient == nil {
		return
	}
	key := types.NamespacedName{Namespace: namespace, Name: name}
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		var latest gatewayapi.Gateway
		if err := StatusClient.Get(wctx, key, &latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		if !applyConditions(&latest.Status.Conditions, build(latest.Generation)) {
			return nil
		}
		return StatusClient.Status().Update(wctx, &latest)
	})
	if err != nil {
		utils.LogFromContext(ctx).Warnf("failed to update status of gateway %s: %s", key, err.Error())
	}
}

func updateListenerSetConditions(ctx context.Context, namespace, name string, build func(gen int64) []metav1.Condition) {
	if StatusClient == nil {
		return
	}
	key := types.NamespacedName{Namespace: namespace, Name: name}
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		var latest gatewayapi.ListenerSet
		if err := StatusClient.Get(wctx, key, &latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		if !applyConditions(&latest.Status.Conditions, build(latest.Generation)) {
			return nil
		}
		return StatusClient.Status().Update(wctx, &latest)
	})
	if err != nil {
		utils.LogFromContext(ctx).Warnf("failed to update status of listenerset %s: %s", key, err.Error())
	}
}

// applyConditions sets each of conds on *existing and reports whether anything other than
// LastTransitionTime bookkeeping changed.
func applyConditions(existing *[]metav1.Condition, conds []metav1.Condition) bool {
	changed := false
	for _, c := range conds {
		old := meta.FindStatusCondition(*existing, c.Type)
		if old == nil || old.Status != c.Status || old.Reason != c.Reason ||
			old.Message != c.Message || old.ObservedGeneration != c.ObservedGeneration {
			changed = true
		}
		meta.SetStatusCondition(existing, c)
	}
	return changed
}

func parentStatusesEqual(a, b []gatewayapi.RouteParentStatus) bool {
	if len(a) != len(b) {
		return false
	}
	strip := func(ps []gatewayapi.RouteParentStatus) []gatewayapi.RouteParentStatus {
		out := make([]gatewayapi.RouteParentStatus, len(ps))
		for i, p := range ps {
			out[i] = *p.DeepCopy()
			for j := range out[i].Conditions {
				out[i].Conditions[j].LastTransitionTime = metav1.Time{}
			}
		}
		return out
	}
	return reflect.DeepEqual(strip(a), strip(b))
}

// maxConditionMessage keeps messages (AS3 errors can be long) well inside the API's limit.
const maxConditionMessage = 1024

func condition[T ~string, R ~string](condType T, status bool, reason R, message string, gen int64) metav1.Condition {
	s := metav1.ConditionFalse
	if status {
		s = metav1.ConditionTrue
	}
	if len(message) > maxConditionMessage {
		message = message[:maxConditionMessage-3] + "..."
	}
	return metav1.Condition{
		Type:               string(condType),
		Status:             s,
		Reason:             string(reason),
		Message:            message,
		ObservedGeneration: gen,
		LastTransitionTime: metav1.Now(),
	}
}

func gatewayHasAddress(gw *gatewayapi.Gateway) bool {
	return len(gw.Spec.Addresses) > 0 || len(gw.Status.Addresses) > 0
}
