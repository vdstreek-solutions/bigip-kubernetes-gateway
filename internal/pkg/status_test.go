package pkg

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/f5devcentral/f5-bigip-rest-go/utils"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayapi "sigs.k8s.io/gateway-api/apis/v1"
)

const testClass = "test-class"

// setupTestClass caches a GatewayClass owned by this controller, and returns a cleanup func
// that removes it plus everything passed to cache().
func setupTestClass(t *testing.T) (cache func(objs ...interface{}), cleanup func()) {
	savedName := ActiveSIGs.ControllerName
	ActiveSIGs.ControllerName = "test/controller"
	ActiveSIGs.SetGatewayClass(&gatewayapi.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: testClass},
		Spec:       gatewayapi.GatewayClassSpec{ControllerName: "test/controller"},
	})
	undo := []func(){}
	cache = func(objs ...interface{}) {
		for _, o := range objs {
			switch obj := o.(type) {
			case *gatewayapi.Gateway:
				ActiveSIGs.SetGateway(obj)
				undo = append(undo, func() { ActiveSIGs.UnsetGateway(utils.Keyname(obj.Namespace, obj.Name)) })
			case *gatewayapi.HTTPRoute:
				ActiveSIGs.SetHTTPRoute(obj)
				undo = append(undo, func() {
					ActiveSIGs.UnsetHTTPRoute(utils.Keyname(obj.Namespace, obj.Name))
					ForgetHTTPRoute(obj.Namespace, obj.Name)
				})
			default:
				t.Fatalf("unsupported fixture %T", o)
			}
		}
	}
	cleanup = func() {
		for _, u := range undo {
			u()
		}
		ActiveSIGs.UnsetGatewayClass(testClass)
		ActiveSIGs.ControllerName = savedName
	}
	return cache, cleanup
}

func testGateway(name string, listeners ...gatewayapi.Listener) *gatewayapi.Gateway {
	ipType := gatewayapi.IPAddressType
	return &gatewayapi.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "gw-ns", Name: name},
		Spec: gatewayapi.GatewaySpec{
			GatewayClassName: testClass,
			Addresses:        []gatewayapi.GatewayAddress{{Type: &ipType, Value: "10.0.0.1"}},
			Listeners:        listeners,
		},
	}
}

func httpListener(name string, port int) gatewayapi.Listener {
	all := gatewayapi.NamespacesFromAll
	return gatewayapi.Listener{
		Name:          gatewayapi.SectionName(name),
		Port:          gatewayapi.PortNumber(port),
		Protocol:      gatewayapi.HTTPProtocolType,
		AllowedRoutes: &gatewayapi.AllowedRoutes{Namespaces: &gatewayapi.RouteNamespaces{From: &all}},
	}
}

func testRoute(name, gateway string, annotations map[string]string) *gatewayapi.HTTPRoute {
	gwNS := gatewayapi.Namespace("gw-ns")
	section := gatewayapi.SectionName("http")
	return &gatewayapi.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: name, Annotations: annotations},
		Spec: gatewayapi.HTTPRouteSpec{
			CommonRouteSpec: gatewayapi.CommonRouteSpec{ParentRefs: []gatewayapi.ParentReference{
				{Name: gatewayapi.ObjectName(gateway), Namespace: &gwNS, SectionName: &section},
			}},
			Rules: []gatewayapi.HTTPRouteRule{{BackendRefs: []gatewayapi.HTTPBackendRef{{BackendRef: gatewayapi.BackendRef{
				BackendObjectReference: gatewayapi.BackendObjectReference{Name: gatewayapi.ObjectName(name)},
			}}}}},
		},
	}
}

func virtualIRules(t *testing.T, cfg map[string]interface{}, vsname string) []string {
	t.Helper()
	vs, ok := cfg["serviceMain"].(map[string]interface{})["ltm/virtual/"+vsname].(map[string]interface{})
	if !ok {
		t.Fatalf("virtual %s missing from %v", vsname, cfg)
	}
	irules, _ := vs["iRules"].([]string)
	return irules
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

var badAuth = map[string]string{"f5-bigip/auth-type": "digest"}

func TestParseAllForClass_brokenRouteDoesNotBlockOthers(t *testing.T) {
	cache, cleanup := setupTestClass(t)
	defer cleanup()
	cache(testGateway("gw", httpListener("http", 80)),
		testRoute("good", "gw", nil),
		testRoute("bad", "gw", badAuth))

	cfg, report, err := ParseAllForClass(testClass)
	if err != nil {
		t.Fatalf("a broken route must not fail the class: %s", err)
	}
	folder := cfg["serviceMain"].(map[string]interface{})
	if _, ok := folder["ltm/rule/hr.apps.good"]; !ok {
		t.Errorf("good route's iRule missing")
	}
	if _, ok := folder["ltm/rule/hr.apps.bad"]; ok {
		t.Errorf("bad route's iRule must be left out")
	}
	irules := virtualIRules(t, cfg, "gw.gw-ns.gw.shared-80.0")
	if !contains(irules, "hr.apps.good") || contains(irules, "hr.apps.bad") {
		t.Errorf("virtual must list only buildable route iRules, got %v", irules)
	}
	if res := report.Routes["apps/bad"]; res.Err == nil || res.KeptPrevious {
		t.Errorf("report for bad route: %+v", res)
	}
	if res := report.Routes["apps/good"]; res.Err != nil {
		t.Errorf("report for good route: %+v", res)
	}
	if len(report.Problems()) != 1 {
		t.Errorf("expected one problem, got %v", report.Problems())
	}
}

func TestParseAllForClass_brokenEditKeepsLastGoodRoute(t *testing.T) {
	cache, cleanup := setupTestClass(t)
	defer cleanup()
	cache(testGateway("gw", httpListener("http", 80)), testRoute("app", "gw", nil))
	if _, _, err := ParseAllForClass(testClass); err != nil {
		t.Fatal(err)
	}

	cache(testRoute("app", "gw", badAuth))
	cfg, report, err := ParseAllForClass(testClass)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg["serviceMain"].(map[string]interface{})["ltm/rule/hr.apps.app"]; !ok {
		t.Errorf("last good iRule must stay in the declaration")
	}
	if irules := virtualIRules(t, cfg, "gw.gw-ns.gw.shared-80.0"); !contains(irules, "hr.apps.app") {
		t.Errorf("virtual must keep listing the last good iRule, got %v", irules)
	}
	if res := report.Routes["apps/app"]; res.Err == nil || !res.KeptPrevious {
		t.Errorf("expected error with KeptPrevious, got %+v", res)
	}
}

func TestParseAllForClass_brokenGatewayDoesNotBlockOthers(t *testing.T) {
	cache, cleanup := setupTestClass(t)
	defer cleanup()
	// Two HTTP listeners on one port can't share a Virtual Server (HTTPS only).
	cache(testGateway("broken", httpListener("http", 80), httpListener("other", 80)),
		testGateway("fine", httpListener("http", 8080)))

	cfg, report, err := ParseAllForClass(testClass)
	if err != nil {
		t.Fatalf("a broken gateway must not fail the class: %s", err)
	}
	folder := cfg["serviceMain"].(map[string]interface{})
	if _, ok := folder["ltm/virtual/gw.gw-ns.fine.shared-8080.0"]; !ok {
		t.Errorf("working gateway's virtual missing: %v", folder)
	}
	for k := range folder {
		if contains([]string{"ltm/virtual/gw.gw-ns.broken.shared-80.0"}, k) {
			t.Errorf("broken gateway's objects must be left out, found %s", k)
		}
	}
	if report.Gateways["gw-ns/broken"] == nil || report.Gateways["gw-ns/fine"] != nil {
		t.Errorf("unexpected gateway report: %v", report.Gateways)
	}
}

func TestHandleBackends_unusedNamespaceDeploysNothing(t *testing.T) {
	saved := PendingDeploys
	PendingDeploys = utils.NewDeployQueue()
	defer func() { PendingDeploys = saved }()

	ActiveSIGs.SetService(&v1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "unused", Name: "svc"}})
	defer ActiveSIGs.UnsetService("unused/svc")

	if err := HandleBackends(NewContext(), "unused"); err != nil {
		t.Fatal(err)
	}
	if n := PendingDeploys.Len(); n != 0 {
		t.Errorf("expected no deploy for a namespace no Gateway uses, got %d queued", n)
	}
}

func TestApplyConditions_onlyReportsRealChanges(t *testing.T) {
	conds := []metav1.Condition{}
	c := condition(gatewayapi.GatewayConditionProgrammed, true, gatewayapi.GatewayReasonProgrammed, "Deployed to BIG-IP", 1)
	if !applyConditions(&conds, []metav1.Condition{c}) {
		t.Errorf("first set must report a change")
	}
	again := condition(gatewayapi.GatewayConditionProgrammed, true, gatewayapi.GatewayReasonProgrammed, "Deployed to BIG-IP", 1)
	if applyConditions(&conds, []metav1.Condition{again}) {
		t.Errorf("identical condition (new timestamp only) must not count as a change, or status writes would loop")
	}
	failed := condition(gatewayapi.GatewayConditionProgrammed, false, gatewayapi.GatewayReasonInvalid, "AS3 deploy failed", 1)
	if !applyConditions(&conds, []metav1.Condition{failed}) {
		t.Errorf("status flip must report a change")
	}
}

func TestStatusWriting(t *testing.T) {
	cache, cleanup := setupTestClass(t)
	defer cleanup()

	scheme := runtime.NewScheme()
	if err := gatewayapi.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	gw := testGateway("gw", httpListener("http", 80))
	good, bad := testRoute("good", "gw", nil), testRoute("bad", "gw", badAuth)
	// Another controller's status entry on the same route must survive our writes.
	other := gatewayapi.RouteParentStatus{
		ParentRef:      gatewayapi.ParentReference{Name: "cilium-gw"},
		ControllerName: "io.cilium/gateway-controller",
		Conditions:     []metav1.Condition{condition("Accepted", true, "Accepted", "", 1)},
	}
	good.Status.Parents = []gatewayapi.RouteParentStatus{other}
	StatusClient = fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(gw.DeepCopy(), good.DeepCopy(), bad.DeepCopy()).
		WithStatusSubresource(&gatewayapi.Gateway{}, &gatewayapi.HTTPRoute{}).Build()
	defer func() { StatusClient = nil }()
	cache(gw, good, bad)

	_, report, err := ParseAllForClass(testClass)
	if err != nil {
		t.Fatal(err)
	}
	storeClassReport(report)
	WriteParseStatus(context.TODO(), report)
	OnDeployFinished([]string{testClass}, nil)

	get := func(obj client.Object, name string) {
		t.Helper()
		if err := StatusClient.Get(context.TODO(), types.NamespacedName{Namespace: obj.GetNamespace(), Name: name}, obj); err != nil {
			t.Fatal(err)
		}
	}
	var gotGW gatewayapi.Gateway
	gotGW.Namespace = "gw-ns"
	get(&gotGW, "gw")
	for _, ct := range []string{"Accepted", "Programmed"} {
		if c := meta.FindStatusCondition(gotGW.Status.Conditions, ct); c == nil || c.Status != metav1.ConditionTrue {
			t.Errorf("gateway %s: %+v", ct, c)
		}
	}

	var gotGood, gotBad gatewayapi.HTTPRoute
	gotGood.Namespace, gotBad.Namespace = "apps", "apps"
	get(&gotGood, "good")
	get(&gotBad, "bad")
	if len(gotGood.Status.Parents) != 2 || gotGood.Status.Parents[0].ControllerName != other.ControllerName {
		t.Errorf("other controller's entry must be kept, ours added: %+v", gotGood.Status.Parents)
	}
	if c := meta.FindStatusCondition(gotGood.Status.Parents[1].Conditions, "Accepted"); c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("good route Accepted: %+v", c)
	}
	if c := meta.FindStatusCondition(gotBad.Status.Parents[0].Conditions, "Accepted"); c == nil ||
		c.Status != metav1.ConditionFalse || c.Reason != string(gatewayapi.RouteReasonUnsupportedValue) {
		t.Errorf("bad route Accepted: %+v", c)
	}
	// The backend Service isn't cached in this test.
	if c := meta.FindStatusCondition(gotBad.Status.Parents[0].Conditions, "ResolvedRefs"); c == nil ||
		c.Reason != string(gatewayapi.RouteReasonBackendNotFound) {
		t.Errorf("bad route ResolvedRefs: %+v", c)
	}

	// Writing the same outcome again must not touch the objects, or every status write would
	// retrigger reconciles forever.
	versions := []string{gotGW.ResourceVersion, gotGood.ResourceVersion, gotBad.ResourceVersion}
	WriteParseStatus(context.TODO(), report)
	OnDeployFinished([]string{testClass}, nil)
	get(&gotGW, "gw")
	get(&gotGood, "good")
	get(&gotBad, "bad")
	if now := []string{gotGW.ResourceVersion, gotGood.ResourceVersion, gotBad.ResourceVersion}; !reflect.DeepEqual(now, versions) {
		t.Errorf("unchanged status was rewritten: %v -> %v", versions, now)
	}

	OnDeployFinished([]string{testClass}, []error{fmt.Errorf("422 from AS3")})
	get(&gotGW, "gw")
	if c := meta.FindStatusCondition(gotGW.Status.Conditions, "Programmed"); c == nil || c.Status != metav1.ConditionFalse {
		t.Errorf("failed deploy must set Programmed=False: %+v", c)
	}
}

func TestLogProblem_logsOncePerChangeAndOnResolve(t *testing.T) {
	key := "test problem"
	defer LogProblem(key, "")
	logged := func() (string, bool) {
		loggedProblemsMu.Lock()
		defer loggedProblemsMu.Unlock()
		m, ok := loggedProblems[key]
		return m, ok
	}
	LogProblem(key, "broken")
	if m, ok := logged(); !ok || m != "broken" {
		t.Fatalf("problem not recorded: %q %v", m, ok)
	}
	LogProblem(key, "broken differently")
	if m, _ := logged(); m != "broken differently" {
		t.Errorf("changed message not recorded: %q", m)
	}
	LogProblem(key, "")
	if _, ok := logged(); ok {
		t.Errorf("resolved problem must be cleared")
	}
}

// Problems must reach the log even when status can't be written at all.
func TestWriteParseStatus_logsWithoutStatusClient(t *testing.T) {
	cache, cleanup := setupTestClass(t)
	defer cleanup()
	StatusClient = nil
	cache(testGateway("gw", httpListener("http", 80)), testRoute("bad", "gw", badAuth))

	_, report, err := ParseAllForClass(testClass)
	if err != nil {
		t.Fatal(err)
	}
	WriteParseStatus(context.TODO(), report)

	key := "httproute apps/bad (parent gw-ns/gw/http)"
	defer LogProblem(key, "")
	loggedProblemsMu.Lock()
	msg := loggedProblems[key]
	loggedProblemsMu.Unlock()
	if msg == "" || !strings.Contains(msg, "auth-type") {
		t.Errorf("expected the route's build error to be logged, got %q", msg)
	}
}
