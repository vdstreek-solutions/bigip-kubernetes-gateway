package pkg

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	gatewayapi "sigs.k8s.io/gateway-api/apis/v1"
)

func Test_parseiRulesFrom(t *testing.T) {
	hryaml := `
apiVersion: gateway.networking.k8s.io/v1beta1
kind: HTTPRoute
metadata:
  name: myhttproute
  namespace: default
spec:
  parentRefs:
    - name: mygateway
      sectionName: http
  hostnames:
    - gateway.test.automation
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /test1
      backendRefs:
        - name: coffee
          port: 80
          weight: 1
        - name: tea
          port: 80
          weight: 9
    - matches:
        - path:
            type: PathPrefix
            value: /test2
      backendRefs:
        - name: coffee
          port: 80
          weight: 9
        - name: tea
          port: 80
          weight: 1
`

	var hr gatewayapi.HTTPRoute
	if err := load2runtimeObject([]byte(hryaml), &hr); err != nil {
		t.Logf("failed with msg: %s", err.Error())
		t.Fail()
	}

	rlt := map[string]interface{}{}
	if err := parseiRulesFrom("bigip", &hr, rlt); err != nil {
		t.Logf("failed with msg: %s", err.Error())
		t.Fail()
	}
	if raw, err := json.MarshalIndent(rlt, "", "  "); err != nil {
		t.Fail()
	} else {
		t.Logf("%s", raw)
		// TODO: do kinds of checks here.
	}
}

// excludedHosts must list dex's exact hostname for the wildcard catch-all route sharing its
// Gateway, and must be empty for dex's own exact-hostname route itself -- this is what lets the
// catch-all's own generated iRule refuse to act on dex's traffic regardless of which order BIG-IP
// actually executes the two routes' iRules in (confirmed live: it does NOT reliably honor the
// declared `rules` list order when only that order, not membership, changes -- see this
// function's own doc comment in iruletmpl_parser.go).
func Test_excludedHosts(t *testing.T) {
	gwYaml := `
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: gw-test
  namespace: default
spec:
  gatewayClassName: test-class
  listeners:
    - name: https
      port: 443
      protocol: HTTPS
      allowedRoutes:
        namespaces:
          from: All
`
	exactYaml := `
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: dex
  namespace: kube-system
spec:
  parentRefs:
    - name: gw-test
      namespace: default
      sectionName: https
  hostnames:
    - dex.kubernetes-dev.wurnet.nl
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /dex
      backendRefs:
        - name: dex
          port: 5556
`
	wildcardYaml := `
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: global-intercept-private
  namespace: wur
spec:
  parentRefs:
    - name: gw-test
      namespace: default
      sectionName: https
  hostnames:
    - "*.kubernetes-dev.wurnet.nl"
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /
      backendRefs:
        - name: global-intercept-svc
          port: 80
`
	var gw gatewayapi.Gateway
	var exactHR, wildcardHR gatewayapi.HTTPRoute
	for _, pair := range []struct {
		data []byte
		obj  runtime.Object
	}{{[]byte(gwYaml), &gw}, {[]byte(exactYaml), &exactHR}, {[]byte(wildcardYaml), &wildcardHR}} {
		if err := load2runtimeObject(pair.data, pair.obj); err != nil {
			t.Fatalf("failed to load fixture: %s", err.Error())
		}
	}

	ActiveSIGs.SetGateway(&gw)
	defer ActiveSIGs.UnsetGateway("default/gw-test")
	ActiveSIGs.SetHTTPRoute(&exactHR)
	defer ActiveSIGs.UnsetHTTPRoute("kube-system/dex")
	ActiveSIGs.SetHTTPRoute(&wildcardHR)
	defer ActiveSIGs.UnsetHTTPRoute("wur/global-intercept-private")

	got := excludedHosts(&wildcardHR)
	if len(got) != 1 || got[0] != "dex.kubernetes-dev.wurnet.nl" {
		t.Errorf("wildcard route's excludedHosts = %v, want [dex.kubernetes-dev.wurnet.nl]", got)
	}

	if got := excludedHosts(&exactHR); len(got) != 0 {
		t.Errorf("exact-hostname route's excludedHosts = %v, want empty (nothing to exclude for)", got)
	}

	// End-to-end: the wildcard route's OWN compiled iRule must contain a guard refusing dex's
	// exact hostname, regardless of where BIG-IP actually places this iRule in its real
	// execution chain relative to dex's own.
	rlt := map[string]interface{}{}
	if err := parseiRulesFrom("bigip", &wildcardHR, rlt); err != nil {
		t.Fatalf("parseiRulesFrom(wildcardHR): %s", err)
	}
	name := hrName(&wildcardHR)
	rule, ok := rlt["ltm/rule/"+name].(map[string]interface{})
	if !ok {
		t.Fatalf("no ltm/rule/%s in %v", name, rlt)
	}
	irule, _ := rule["iRule"].(string)
	t.Logf("compiled wildcard iRule:\n%s", irule)
	if !strings.Contains(irule, "lsearch -exact") || !strings.Contains(irule, "dex.kubernetes-dev.wurnet.nl") {
		t.Errorf("compiled wildcard iRule is missing the dex exclusion guard:\n%s", irule)
	}

	// The exact-hostname route's own iRule must NOT gain any guard -- it has nothing to defer to.
	rlt2 := map[string]interface{}{}
	if err := parseiRulesFrom("bigip", &exactHR, rlt2); err != nil {
		t.Fatalf("parseiRulesFrom(exactHR): %s", err)
	}
	exactIRule, _ := rlt2["ltm/rule/"+hrName(&exactHR)].(map[string]interface{})["iRule"].(string)
	if strings.Contains(exactIRule, "lsearch -exact") {
		t.Errorf("exact-hostname route's own iRule should have no exclusion guard:\n%s", exactIRule)
	}
}

func yaml2json(data []byte) ([]byte, error) {
	var intf interface{}
	if err := yaml.Unmarshal(data, &intf); err != nil {
		return nil, err
	}

	if out, err := json.Marshal(intf); err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

func load2runtimeObject(yamldata []byte, obj runtime.Object) error {
	if jdata, err := yaml2json(yamldata); err != nil {
		return err
	} else {
		_, _, err := unstructured.UnstructuredJSONScheme.Decode(jdata, nil, obj)
		if err != nil {
			return err
		}
	}
	return nil
}

// Two same-named Services in different namespaces must not collide on one pool once a shared
// partition is configured -- and the iRule must reference the same names ParseServices declares.
func Test_poolNamesDoNotCollideAcrossNamespaces(t *testing.T) {
	savedConfigs := BIPConfigs
	BIPConfigs = BIGIPConfigs{{Partition: "K8S-D"}}
	defer func() { BIPConfigs = savedConfigs }()

	for _, ns := range []string{"app-a", "app-b"} {
		svc := &v1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "frontend"},
			Spec:       v1.ServiceSpec{Type: v1.ServiceTypeNodePort},
		}
		ActiveSIGs.SetService(svc)
		ActiveSIGs.SetEndpoints(&v1.Endpoints{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "frontend"}})
		defer ActiveSIGs.UnsetService(ns + "/frontend")
		defer ActiveSIGs.UnsetEndpoints(ns + "/frontend")
	}

	rlts, _, err := ParseServices([]string{"app-a/frontend", "app-b/frontend"})
	if err != nil {
		t.Fatalf("ParseServices: %s", err)
	}
	folder := rlts["K8S-D"].(map[string]interface{})["serviceMain"].(map[string]interface{})
	for _, want := range []string{"ltm/pool/pool.app-a.frontend", "ltm/pool/pool.app-b.frontend"} {
		if _, ok := folder[want]; !ok {
			t.Errorf("missing %s in %v", want, folder)
		}
	}
	if len(folder) != 2 {
		t.Errorf("expected 2 pools, got %d: %v", len(folder), folder)
	}

	for _, ns := range []string{"app-a", "app-b"} {
		hr := &gatewayapi.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "route"}}
		backends := []gatewayapi.HTTPBackendRef{{BackendRef: gatewayapi.BackendRef{
			BackendObjectReference: gatewayapi.BackendObjectReference{Name: "frontend"},
		}}}
		got := parsePoolweight(backends, hr)
		want := "/K8S-D/serviceMain/pool." + ns + ".frontend 1"
		if got != want {
			t.Errorf("parsePoolweight for %s: got %q, want %q", ns, got, want)
		}
	}
}
