package pkg

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/f5devcentral/bigip-kubernetes-gateway/internal/k8s"
	f5_bigip "github.com/f5devcentral/f5-bigip-rest-go/bigip"
	"github.com/f5devcentral/f5-bigip-rest-go/utils"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Test_diffPodARPs(t *testing.T) {
	desired := map[string]string{
		"10.160.5.247": "0a:0a:0a:7e:82:51", // already right
		"10.160.6.148": "0a:0a:0a:7e:82:52", // wrong MAC on BIG-IP
		"10.160.3.6":   "0a:0a:0a:7e:82:47", // missing
	}
	existing := map[string][2]string{
		"k8s-pod-10.160.5.247": {"10.160.5.247", "0A:0A:0A:7E:82:51"},
		"k8s-pod-10.160.6.148": {"10.160.6.148", "0a:0a:0a:7e:82:48"},
		"k8s-pod-10.160.9.9":   {"10.160.9.9", "0a:0a:0a:7e:82:48"}, // pod gone
		"k8s-pod-renamed":      {"10.160.3.6", "0a:0a:0a:7e:82:47"}, // ours, but not named after its IP
		"manual-arp":           {"10.160.7.7", "aa:bb:cc:dd:ee:ff"}, // not ours
	}
	got := diffPodARPs(desired, existing)
	want := podARPChanges{
		create: map[string]string{"10.160.3.6": "0a:0a:0a:7e:82:47"},
		update: map[string]string{"10.160.6.148": "0a:0a:0a:7e:82:52"},
		remove: []string{"k8s-pod-10.160.9.9", "k8s-pod-renamed"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

// fakeARPBIGIP serves just enough iControl REST for f5_bigip.New and the net/arp calls, and
// keeps ARP entries keyed by full path (/<partition>/<name>).
type fakeARPBIGIP struct {
	mu   sync.Mutex
	arps map[string]map[string]interface{}
}

func (f *fakeARPBIGIP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := r.URL.Path
	switch {
	case path == "/mgmt/tm/sys/version":
		json.NewEncoder(w).Encode(map[string]interface{}{"entries": map[string]interface{}{
			"https://localhost/mgmt/tm/sys/version/0": map[string]interface{}{"nestedStats": map[string]interface{}{
				"entries": map[string]interface{}{"Version": map[string]interface{}{"description": "17.5.1"}}}}}})
	case strings.HasPrefix(path, "/mgmt/tm/sys/folder"):
		json.NewEncoder(w).Encode(map[string]interface{}{})
	case path == "/mgmt/tm/net/arp" && r.Method == http.MethodGet:
		// Deliberately ignores $filter, to prove entries of other partitions are left alone.
		items := []interface{}{}
		for _, a := range f.arps {
			items = append(items, a)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"items": items})
	case path == "/mgmt/tm/net/arp" && r.Method == http.MethodPost:
		var body map[string]interface{}
		b, _ := io.ReadAll(r.Body)
		json.Unmarshal(b, &body)
		f.arps["/"+body["partition"].(string)+"/"+body["name"].(string)] = body
	case strings.HasPrefix(path, "/mgmt/tm/net/arp/"):
		ref, _ := url.PathUnescape(strings.TrimPrefix(r.URL.EscapedPath(), "/mgmt/tm/net/arp/"))
		key := strings.ReplaceAll(ref, "~", "/")
		a, ok := f.arps[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		switch r.Method {
		case http.MethodPatch:
			var body map[string]interface{}
			b, _ := io.ReadAll(r.Body)
			json.Unmarshal(b, &body)
			for k, v := range body {
				a[k] = v
			}
		case http.MethodDelete:
			delete(f.arps, key)
		}
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeARPBIGIP) macs() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	rlt := map[string]string{}
	for k, a := range f.arps {
		rlt[k] = a["ipAddress"].(string) + " " + a["macAddress"].(string)
	}
	return rlt
}

func arp(partition, name, ip, mac string) map[string]interface{} {
	return map[string]interface{}{"partition": partition, "name": name, "ipAddress": ip, "macAddress": mac}
}

func testNode(t *testing.T, name, ip string) {
	t.Helper()
	err := k8s.NodeCache.Set(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{{Reason: "CiliumIsUp"}},
			Addresses:  []v1.NodeAddress{{Type: v1.NodeInternalIP, Address: ip}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { k8s.NodeCache.Unset(name) })
}

func TestSyncPodARPs(t *testing.T) {
	cache, cleanup := setupTestClass(t)
	defer cleanup()
	cache(testGateway("gw", httpListener("http", 80)), testRoute("web", "gw", nil))

	ActiveSIGs.SetService(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "web"},
		Spec:       v1.ServiceSpec{Type: v1.ServiceTypeClusterIP, Ports: []v1.ServicePort{{Name: "http", Port: 80}}},
	})
	defer ActiveSIGs.UnsetService(utils.Keyname("apps", "web"))
	nodeA, nodeB := "node-a", "node-b"
	testNode(t, nodeA, "10.126.130.81")
	testNode(t, nodeB, "10.126.130.82")
	ActiveSIGs.SetEndpoints(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "web"},
		Subsets: []v1.EndpointSubset{{
			Ports: []v1.EndpointPort{{Name: "http", Port: 8080}},
			Addresses: []v1.EndpointAddress{
				{IP: "10.160.5.247", NodeName: &nodeA},
				{IP: "10.160.6.148", NodeName: &nodeB},
				{IP: "10.126.130.82", NodeName: &nodeB}, // host network: no entry
			},
		}},
	})
	defer ActiveSIGs.UnsetEndpoints(utils.Keyname("apps", "web"))

	fake := &fakeARPBIGIP{arps: map[string]map[string]interface{}{
		"/K8S-D/k8s-pod-10.160.6.148": arp("K8S-D", "k8s-pod-10.160.6.148", "10.160.6.148", "0a:0a:0a:7e:82:48"),
		"/K8S-D/k8s-pod-10.160.9.9":   arp("K8S-D", "k8s-pod-10.160.9.9", "10.160.9.9", "0a:0a:0a:7e:82:48"),
		"/K8S-D/manual-arp":           arp("K8S-D", "manual-arp", "10.160.7.7", "aa:bb:cc:dd:ee:ff"),
		"/K8S-T/k8s-pod-10.162.6.8":   arp("K8S-T", "k8s-pod-10.162.6.8", "10.162.6.8", "0a:0a:0a:7e:82:88"),
	}}
	srv := httptest.NewTLSServer(fake)
	defer srv.Close()

	savedBIGIPs, savedConfigs := BIGIPs, BIPConfigs
	defer func() { BIGIPs, BIPConfigs = savedBIGIPs, savedConfigs }()
	BIGIPs = []*f5_bigip.BIGIP{f5_bigip.New(srv.URL, "u", "p")}
	cfg := BIGIPConfig{}
	cfg.Vtep.TunnelName, cfg.Vtep.TunnelPartition = "cilium-vxlan", "K8S-D"
	BIPConfigs = BIGIPConfigs{cfg}

	if err := SyncPodARPs(NewContext()); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"/K8S-D/k8s-pod-10.160.5.247": "10.160.5.247 0a:0a:0a:7e:82:51",
		"/K8S-D/k8s-pod-10.160.6.148": "10.160.6.148 0a:0a:0a:7e:82:52",
		"/K8S-D/manual-arp":           "10.160.7.7 aa:bb:cc:dd:ee:ff",
		"/K8S-T/k8s-pod-10.162.6.8":   "10.162.6.8 0a:0a:0a:7e:82:88",
	}
	if got := fake.macs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ARP entries after sync:\n got %v\nwant %v", got, want)
	}

	// Nothing changed: the second pass must not write.
	before := fake.macs()
	if err := SyncPodARPs(NewContext()); err != nil {
		t.Fatal(err)
	}
	if got := fake.macs(); !reflect.DeepEqual(got, before) {
		t.Fatalf("second sync changed entries: %v", got)
	}
}
