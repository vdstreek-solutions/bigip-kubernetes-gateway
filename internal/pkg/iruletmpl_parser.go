package pkg

import (
	"bytes"
	"embed"
	"encoding/base64"
	"fmt"
	"net"
	"sort"
	"strings"
	"text/template"

	"github.com/f5devcentral/f5-bigip-rest-go/utils"
	gatewayapi "sigs.k8s.io/gateway-api/apis/v1"
)

var iruleTemplate *template.Template

// issues: https://github.com/golang/go/issues/46056
// I have to move the templates to this directory.

//go:embed irule_templates/*.tmpl
var tmpls embed.FS

func init() {

	funcs := template.FuncMap{
		"orHosts":          orHostnames,
		"parseMatches":     parseiRuleMatches,
		"parseReqFilters":  parseiRuleReqFilters,
		"parseRespFilters": parseiRuleRespFilters,
		"parsePoolweight":  parsePoolweight,
		"tclSafeName":      tclSafeName,
		"poolVarPrefix":    poolVarPrefix,
		"parseIPWhitelist": parseIPWhitelist,
		"parseAuth":        parseAuth,
		"excludedHosts":    excludedHosts,
	}

	iruleTemplate = template.Must(template.New("").Funcs(funcs).ParseFS(tmpls, "irule_templates/*.tmpl"))
}

// tclSafeName makes a K8s name/namespace safe to use as part of an UNBRACED Tcl variable
// reference (e.g. $static::pools_argocd_argocd-server_0). Tcl's `$name` shorthand only recognizes
// letters, digits, underscore and `::` as part of the name -- a literal hyphen (valid and common
// in K8s names, e.g. "argocd-server", "f5-private") would silently truncate the substitution
// there instead of erroring, so this can't be caught by testing with a hyphen-free name alone.
func tclSafeName(s string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, s)
}

// poolVarPrefix builds the per-route Tcl pool-array prefix as static::pools_ns<N>_<ns>_<name>,
// where N is the RAW (pre-sanitize) byte length of the namespace. tclSafeName maps a hyphen to
// the exact same '_' used to join the namespace and name, so joining two tclSafeName'd components
// with a plain "_" is ambiguous wherever a hyphen lands on that boundary: {ns: "kasten-io", name:
// "kasten"} and {ns: "kasten", name: "io-kasten"} both produce "static::pools_kasten_io_kasten".
// Since Kubernetes namespace/name are byte-for-byte ASCII (DNS-1123), tclSafeName's rune-for-rune
// mapping never changes a string's length -- so a length prefix for the namespace component fixes
// exactly where it ends, the same way a length-prefixed (Pascal/netstring-style) encoding avoids
// delimiter ambiguity for any variable-length string. Confirmed live once already for the
// rule-index-only version of this array name (see this file's history) -- namespace/name made it
// unique per-route, but not yet unambiguous at the hyphen boundary.
//
// It also starts with the route's partition. static:: is shared by every iRule on the BIG-IP, not
// just this controller's, so two clusters on one BIG-IP with the same HTTPRoute (dev and tst both
// have argocd/argocd-server) wrote the same array, and whichever iRule loaded last won. Confirmed
// live: K8S-D's virtual server sent requests to a K8S-T pool, whose tst pod dev's tunnel can't
// reach, and the client got a reset after "{peer} TCP retransmit timeout".
func poolVarPrefix(hr *gatewayapi.HTTPRoute) string {
	return fmt.Sprintf("static::pools_p%s_ns%d_%s_%s", tclEscape(ResolvePartition(hr.Namespace)),
		len(hr.Namespace), tclSafeName(hr.Namespace), tclSafeName(hr.Name))
}

// tclEscape makes any string Tcl-variable-safe without losing information, unlike tclSafeName:
// a partition name may contain both '-' and '_', so "K8S-D" and "K8S_D" must not end up the same.
// Every character other than a letter or digit becomes '_' plus two hex digits ("K8S-D" ->
// "K8S_2dD"). The result never contains "_n", so it can't run into the "_ns" that follows it.
func tclEscape(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			for _, c := range []byte(string(r)) {
				fmt.Fprintf(&b, "_%02x", c)
			}
		}
	}
	return b.String()
}

// excludedHosts returns every EXACT hostname (e.g. "dex.kubernetes-dev.wurnet.nl") from another
// AttachedHTTPRoute on the same Gateway(s) as hr that falls under one of hr's OWN wildcard
// hostnames -- i.e. hosts that already have their own dedicated route and must not be served by
// this wildcard catch-all. Returns nil for an hr with no wildcard hostname (an exact-hostname
// route never needs to defer to anything).
//
// This exists instead of relying on BIG-IP executing a shared Virtual Server's merged iRules in
// the order parseGateway declares them (hostname-specific routes before a wildcard catch-all,
// via routeNameSpecificity) -- confirmed live that TMOS does NOT actually re-order its compiled
// execution chain when an AS3 "selective" deploy changes only the ORDER of an already-identical
// `rules` membership list (tmsh showed the corrected order; the catch-all still ran first and
// swallowed every other app's traffic regardless). A plain full-membership rules replace (one
// member removed, so membership genuinely changed) immediately fixed execution order, confirming
// the Go-side ordering logic itself was always correct and the bug is BIG-IP's, not ours -- but a
// fix that depends on BIG-IP re-deriving execution order from list order isn't reliable, so the
// wildcard route's own generated iRule has to refuse to act on an owned host itself, regardless
// of where in the chain it actually runs.
func excludedHosts(hr *gatewayapi.HTTPRoute) []string {
	hasWildcard := false
	for _, h := range hr.Spec.Hostnames {
		if strings.HasPrefix(string(h), "*.") {
			hasWildcard = true
			break
		}
	}
	if !hasWildcard {
		return nil
	}

	seen := map[string]bool{}
	result := []string{}
	for i := range hr.Spec.ParentRefs {
		gw, attached := ActiveSIGs.ParentRefTarget(hr, &hr.Spec.ParentRefs[i])
		if !attached || gw == nil {
			continue
		}
		for _, other := range ActiveSIGs.AttachedHTTPRoutes(gw) {
			if other == hr {
				continue
			}
			for _, oh := range other.Spec.Hostnames {
				ohs := string(oh)
				if strings.HasPrefix(ohs, "*.") || seen[ohs] {
					continue
				}
				for _, wh := range hr.Spec.Hostnames {
					if hostnameCovers(string(wh), ohs) {
						seen[ohs] = true
						result = append(result, ohs)
						break
					}
				}
			}
		}
	}
	sort.Strings(result)
	return result
}

// hostnameCovers reports whether wildcard (e.g. "*.kubernetes-dev.wurnet.nl") matches exact (e.g.
// "dex.kubernetes-dev.wurnet.nl"), mirroring Gateway API's own wildcard-hostname semantics: "*."
// matches any single-or-more-label prefix, never the bare suffix itself. A non-wildcard
// "wildcard" value falls back to plain equality.
func hostnameCovers(wildcard, exact string) bool {
	suffix := strings.TrimPrefix(wildcard, "*")
	if suffix == wildcard {
		return wildcard == exact
	}
	return strings.HasSuffix(exact, suffix) && len(exact) > len(suffix)
}

func orHostnames(prefix string, hostnames []gatewayapi.Hostname) string {
	conditions := []string{}
	for _, hst := range hostnames {
		conditions = append(conditions, fmt.Sprint(prefix, ` "`, hst, `"`))
	}
	ret := strings.Join(conditions, " or ")
	return ret
}

func parseiRuleMatches(matches []gatewayapi.HTTPRouteMatch) string {
	matchConditions := []string{}
	for _, match := range matches {
		singleMatch := []string{}

		if match.Path != nil {
			matchType := gatewayapi.PathMatchPathPrefix
			if match.Path.Type != nil {
				matchType = *match.Path.Type
			}
			switch matchType {
			case gatewayapi.PathMatchPathPrefix:
				singleMatch = append(singleMatch, fmt.Sprintf(`[HTTP::path] starts_with "%s"`, *match.Path.Value))
			case gatewayapi.PathMatchExact:
				singleMatch = append(singleMatch, fmt.Sprintf(`[HTTP::path] eq "%s"`, *match.Path.Value))
			case gatewayapi.PathMatchRegularExpression:
				singleMatch = append(singleMatch, fmt.Sprintf(`[HTTP::path matches "%s"`, *match.Path.Value))
			}
		}
		if match.Headers != nil {
			for _, header := range match.Headers {
				matchType := gatewayapi.HeaderMatchExact
				if header.Type != nil {
					matchType = *header.Type
				}
				switch matchType {
				case gatewayapi.HeaderMatchExact:
					singleMatch = append(singleMatch, fmt.Sprintf(`[HTTP::header "%s"] eq "%s"`, header.Name, header.Value))
				case gatewayapi.HeaderMatchRegularExpression:
					singleMatch = append(singleMatch, fmt.Sprintf(`[HTTP::header "%s"] matches "%s"`, header.Name, header.Value))
				}
			}
		}
		if match.Method != nil {
			singleMatch = append(singleMatch, fmt.Sprintf(`[HTTP::method] eq "%s"`, *match.Method))
		}
		if match.QueryParams != nil {
			for _, queryParam := range match.QueryParams {
				matchType := gatewayapi.QueryParamMatchExact
				if queryParam.Type != nil {
					matchType = *queryParam.Type
				}
				switch matchType {
				case gatewayapi.QueryParamMatchExact:
					singleMatch = append(singleMatch, fmt.Sprintf(`[URI::query [HTTP::uri] "%s"] eq "%s"`, queryParam.Name, queryParam.Value))
				case gatewayapi.QueryParamMatchRegularExpression:
					singleMatch = append(singleMatch, fmt.Sprintf(`[URI::query [HTTP::uri] "%s"] matches "%s"`, queryParam.Name, queryParam.Value))
				}
			}
		}

		matchConditions = append(matchConditions, strings.Join(singleMatch, " and "))
	}
	return strings.Join(matchConditions, " or ")
}

func parseiRuleReqFilters(filters []gatewayapi.HTTPRouteFilter, hr gatewayapi.HTTPRoute) (string, error) {
	reqFilterActions := []string{}
	for _, filter := range filters {
		switch filter.Type {
		case gatewayapi.HTTPRouteFilterRequestHeaderModifier:
			if filter.RequestHeaderModifier != nil {
				for _, mdr := range filter.RequestHeaderModifier.Add {
					reqFilterActions = append(reqFilterActions, fmt.Sprintf("HTTP::header insert %s %s", mdr.Name, mdr.Value))
				}
				for _, mdr := range filter.RequestHeaderModifier.Remove {
					reqFilterActions = append(reqFilterActions, fmt.Sprintf("HTTP::header remove %s", mdr))
				}
				for _, mdr := range filter.RequestHeaderModifier.Set {
					reqFilterActions = append(reqFilterActions, fmt.Sprintf("HTTP::header replace %s %s", mdr.Name, mdr.Value))
				}
			}

		case gatewayapi.HTTPRouteFilterRequestMirror:
			// filter.RequestMirror.BackendRef -> vs mirror?
			return "", fmt.Errorf("filter type '%s' not supported", gatewayapi.HTTPRouteFilterRequestMirror)
		case gatewayapi.HTTPRouteFilterRequestRedirect:
			if rr := filter.RequestRedirect; rr != nil {
				setScheme := `set rscheme "http"`
				if rr.Scheme != nil {
					setScheme = fmt.Sprintf(`set rscheme "%s"`, *rr.Scheme)
				}
				setHostName := `set rhostname "[HTTP::host]"`
				if rr.Hostname != nil {
					setHostName = fmt.Sprintf(`set rhostname "%s"`, *rr.Hostname)
				}

				// experimental .. definition is not clear yet.
				setUri := `set ruri "[HTTP::uri]"`
				if rr.Path != nil && rr.Path.ReplaceFullPath != nil {
					setUri = fmt.Sprintf(`set ruri "%s"`, *rr.Path.ReplaceFullPath)
				}

				// Per the Gateway API spec, an unset Port "MUST be derived using the scheme of
				// the redirect URL" -- i.e. 443 for https, 80 for http -- NOT the port the
				// request arrived on. Reusing TCP::local_port here (the previous default)
				// produces a broken "https://host:80/..." for the extremely common case of an
				// http(80)->https redirect with no explicit port set (true of every
				// *-https-redirect route in this repo), since the request arrives on port 80
				// but the redirect target is https. Confirmed live: cert-manager's ACME HTTP-01
				// self-check followed exactly this redirect and got "connection reset by peer"
				// trying to open TLS against a plaintext port-80 listener. $rscheme is computed
				// at Tcl runtime (may come from rr.Scheme above, or default to "http"), so the
				// port default is resolved the same way, in Tcl, not here in Go.
				setPort := `set rport [expr {[string tolower $rscheme] eq "https" ? 443 : 80}]`
				if rr.Port != nil {
					setPort = fmt.Sprintf(`set rport %d`, *rr.Port)
				}

				if rr.StatusCode != nil {
					if *rr.StatusCode != 301 && *rr.StatusCode != 302 {
						return "", fmt.Errorf("invalid status %d for request redirect", *rr.StatusCode)
					}
				}
				action := fmt.Sprintf(`
					%s
					%s
					%s 
					%s
			        set url $rscheme://$rhostname:$rport$ruri
					HTTP::respond %d Location $url
					`, setScheme, setHostName, setUri, setPort, *rr.StatusCode)
				reqFilterActions = append(reqFilterActions, action)
			}
		case gatewayapi.HTTPRouteFilterExtensionRef:
			if er := filter.ExtensionRef; er != nil {
				pool := fmt.Sprintf("%s.%s", hr.Namespace, er.Name)
				reqFilterActions = append(reqFilterActions, fmt.Sprintf("pool /%s/%s; return", "cis-c-tenant", pool))
			}
		}
	}
	return strings.Join(reqFilterActions, "\n"), nil
}

func parseiRuleRespFilters(filters []gatewayapi.HTTPRouteFilter, hr gatewayapi.HTTPRoute) (string, error) {
	respFilterActions := []string{}
	for _, filter := range filters {
		switch filter.Type {
		case gatewayapi.HTTPRouteFilterResponseHeaderModifier:
			if filter.ResponseHeaderModifier != nil {
				for _, mdr := range filter.ResponseHeaderModifier.Add {
					respFilterActions = append(respFilterActions, fmt.Sprintf("HTTP::header insert %s %s", mdr.Name, mdr.Value))
				}
				for _, mdr := range filter.ResponseHeaderModifier.Remove {
					respFilterActions = append(respFilterActions, fmt.Sprintf("HTTP::header remove %s", mdr))
				}
				for _, mdr := range filter.ResponseHeaderModifier.Set {
					respFilterActions = append(respFilterActions, fmt.Sprintf("HTTP::header replace %s %s", mdr.Name, mdr.Value))
				}
			}
		}
	}
	return strings.Join(respFilterActions, "\n"), nil
}

func parsePoolweight(backends []gatewayapi.HTTPBackendRef, hr *gatewayapi.HTTPRoute) string {
	poolWeights := []string{}
	for _, br := range backends {
		ns := hr.Namespace
		if br.Namespace != nil {
			ns = string(*br.Namespace)
		}
		svc := ActiveSIGs.GetService(utils.Keyname(ns, string(br.Name)))
		if svc != nil && !ActiveSIGs.CanRefer(hr, svc) {
			continue
		}
		// Must resolve the same way ParseServices' rlts[partition] key and pool name do
		// (parser.go), or this iRule ends up referencing a pool path that doesn't exist -- see
		// ResolvePartition and poolName.
		pool := fmt.Sprintf("/%s/serviceMain/%s", ResolvePartition(ns), poolName(ns, string(br.Name)))
		weight := 1
		if br.Weight != nil {
			weight = int(*br.Weight)
		}
		poolWeights = append(poolWeights, fmt.Sprintf("%s %d", pool, weight))
	}

	return strings.Join(poolWeights, " ")
}

// ipWhitelistAnnotation lets a tenant restrict a single app's HTTPRoute to a set of source
// IPs/CIDRs without affecting any other app sharing the same Virtual Server via SNI -- AFM's
// Firewall_Rule_List can't do this, since it acts at connection-accept time on the whole VS,
// before the client-ssl profile (and thus the SNI hostname) is even selected. Enforcing it here,
// inside the already-host/path-matched iRule block, is what makes it per-app.
const ipWhitelistAnnotation = "f5-bigip/allowed-source-ranges"

// parseIPWhitelist builds the client-IP gate for one HTTPRoute from its
// f5-bigip/allowed-source-ranges annotation (comma-separated CIDRs and/or bare IPs). Returns
// "" when the annotation is absent/empty, so existing routes are unaffected.
func parseIPWhitelist(hr gatewayapi.HTTPRoute) (string, error) {
	raw, ok := hr.Annotations[ipWhitelistAnnotation]
	if !ok || strings.TrimSpace(raw) == "" {
		return "", nil
	}

	conditions := []string{}
	for _, entry := range strings.Split(raw, ",") {
		cidr := strings.TrimSpace(entry)
		if cidr == "" {
			continue
		}
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			ip := net.ParseIP(cidr)
			if ip == nil {
				return "", fmt.Errorf("%s: invalid entry %q in annotation %s", utils.Keyname(hr.Namespace, hr.Name), cidr, ipWhitelistAnnotation)
			}
			if ip.To4() != nil {
				cidr += "/32"
			} else {
				cidr += "/128"
			}
		}
		conditions = append(conditions, fmt.Sprintf(`[IP::addr [IP::client_addr] equals %s]`, cidr))
	}
	if len(conditions) == 0 {
		return "", nil
	}

	return fmt.Sprintf(`
    if { not ( %s ) } {
      log local0. "[IP::client_addr] blocked by %s allowlist for %s"
      HTTP::respond 403 content "Forbidden"
      event HTTP_REQUEST disable
      return
    }`, strings.Join(conditions, " or "), ipWhitelistAnnotation, utils.Keyname(hr.Namespace, hr.Name)), nil
}

// authTypeAnnotation and basicAuthSecretAnnotation gate a single app's HTTPRoute behind HTTP
// Basic auth, same per-app scoping rationale as parseIPWhitelist: enforced inside the already
// host/path-matched iRule block, so it only affects this HTTPRoute even when other apps share the
// same Virtual Server via SNI.
const (
	authTypeAnnotation        = "f5-bigip/auth-type"
	basicAuthSecretAnnotation = "f5-bigip/basic-auth-secret"
)

// parseAuth builds the auth gate for one HTTPRoute from its f5-bigip/auth-type annotation.
// Only "basic" is supported today; an unrecognized value errors out instead of silently doing
// nothing, so a typo'd auth-type doesn't deploy as "unprotected". Returns "" when the annotation
// is absent, so existing routes are unaffected.
func parseAuth(hr gatewayapi.HTTPRoute) (string, error) {
	authType, ok := hr.Annotations[authTypeAnnotation]
	if !ok || strings.TrimSpace(authType) == "" {
		return "", nil
	}

	switch authType {
	case "basic":
		return parseBasicAuth(hr)
	default:
		return "", fmt.Errorf("%s: unsupported %s %q (only \"basic\" is supported)",
			utils.Keyname(hr.Namespace, hr.Name), authTypeAnnotation, authType)
	}
}

// parseBasicAuth reads username/password out of the Secret named by basicAuthSecretAnnotation
// (same namespace as the HTTPRoute) and compares the raw Authorization header against the
// expected "Basic <base64>" value computed here -- no Tcl-side b64decode needed. The credential
// ends up readable in the AS3 declaration/iRule on the BIG-IP, same trust tier as the TLS private
// keys this controller already deploys that way; it is not a secret from BIG-IP's own admins.
func parseBasicAuth(hr gatewayapi.HTTPRoute) (string, error) {
	secretName, ok := hr.Annotations[basicAuthSecretAnnotation]
	if !ok || strings.TrimSpace(secretName) == "" {
		return "", fmt.Errorf("%s: %s is \"basic\" but %s is not set",
			utils.Keyname(hr.Namespace, hr.Name), authTypeAnnotation, basicAuthSecretAnnotation)
	}

	keyname := utils.Keyname(hr.Namespace, secretName)
	secret := ActiveSIGs.GetSecret(keyname)
	if secret == nil {
		return "", fmt.Errorf("%s: basic-auth secret %s not found", utils.Keyname(hr.Namespace, hr.Name), keyname)
	}

	username, ok := secret.Data["username"]
	if !ok || len(username) == 0 {
		return "", fmt.Errorf("%s: basic-auth secret %s has no \"username\" key", utils.Keyname(hr.Namespace, hr.Name), keyname)
	}
	password, ok := secret.Data["password"]
	if !ok {
		return "", fmt.Errorf("%s: basic-auth secret %s has no \"password\" key", utils.Keyname(hr.Namespace, hr.Name), keyname)
	}

	expected := "Basic " + base64.StdEncoding.EncodeToString([]byte(string(username)+":"+string(password)))

	return fmt.Sprintf(`
    if { not ( [HTTP::header exists "Authorization"] and [HTTP::header "Authorization"] eq "%s" ) } {
      HTTP::respond 401 WWW-Authenticate {Basic realm="Restricted"} content "Unauthorized"
      event HTTP_REQUEST disable
      return
    }`, expected), nil
}

func parseiRulesFrom(className string, hr *gatewayapi.HTTPRoute, rlt map[string]interface{}) error {
	var tpl bytes.Buffer
	if err := iruleTemplate.ExecuteTemplate(&tpl, "irule.tmpl", hr); err != nil {
		// Keep the cause (e.g. an unsupported f5-bigip/auth-type): it's what ends up in the log
		// and the route's status, and the only way to tell what to fix.
		return fmt.Errorf("cannot build iRule: %w", err)
	}

	name := hrName(hr)
	ruleObj := map[string]interface{}{
		"class": "iRule",
		"iRule": tpl.String(),
	}
	rlt["ltm/rule/"+name] = ruleObj
	return nil
}
