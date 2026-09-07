package execution

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// ValidatePublicHTTPURL guards the HTTP capability bind path against SSRF:
// the endpoint must be http(s) and must resolve only to public, globally
// routable addresses. Loopback, private (RFC1918), link-local, CGNAT,
// multicast, and unspecified addresses are refused, so a tenant-bound
// capability cannot probe the deployment's internal network or the loopback
// stack.
//
// DNS rebinding caveat: validation resolves at bind time. For defense in
// depth, production deployments should also enforce egress policy at the
// network boundary.
func ValidatePublicHTTPURL(raw string) error {
	return validatePublicHTTPURL(raw, net.LookupIP)
}

func validatePublicHTTPURL(raw string, lookup func(string) ([]net.IP, error)) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("invalid endpoint url: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("endpoint scheme must be http or https")
	}
	if parsed.User != nil {
		return fmt.Errorf("endpoint url must not contain user information")
	}
	for key, values := range parsed.Query() {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "secret") || strings.Contains(lower, "password") ||
			strings.Contains(lower, "token") || strings.Contains(lower, "key") ||
			strings.Contains(lower, "auth") || strings.Contains(lower, "credential") {
			for _, value := range values {
				if strings.TrimSpace(value) != "" {
					return fmt.Errorf("endpoint query parameter %q may contain a secret; use a credential header", key)
				}
			}
		}
	}
	host := parsed.Hostname()
	if host == "" {
		return fmt.Errorf("endpoint url has no host")
	}
	addresses := []net.IP{}
	if ip := net.ParseIP(host); ip != nil {
		addresses = append(addresses, ip)
	} else {
		resolved, err := lookup(host)
		if err != nil {
			return fmt.Errorf("endpoint host %q does not resolve", host)
		}
		addresses = append(addresses, resolved...)
	}
	for _, ip := range addresses {
		if !isPubliclyRoutable(ip) {
			return fmt.Errorf("endpoint host %q resolves to a non-public address (%s); internal endpoints are not allowed", host, ip)
		}
	}
	return nil
}

func isPubliclyRoutable(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if v4 := ip.To4(); v4 != nil {
		return isPublicIPv4(v4)
	}
	if !ip.IsGlobalUnicast() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() {
		return false
	}
	if ianaNAT64WellKnown.Contains(ip) {
		return isPublicIPv4(net.IP(ip[12:]))
	}
	if containsCIDR(ianaIPv6NonGlobal, ip) {
		return false
	}
	if ianaIPv6IETF.Contains(ip) {
		return ianaIPv6GlobalException(ip)
	}
	return true
}

func isPublicIPv4(ip net.IP) bool {
	if ip == nil || !ip.IsGlobalUnicast() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsMulticast() {
		return false
	}
	if containsCIDR(ianaIPv4NonGlobal, ip) {
		return ip.Equal(ianaIPv4GlobalException1) || ip.Equal(ianaIPv4GlobalException2)
	}
	return true
}

func ianaIPv6GlobalException(ip net.IP) bool {
	return ip.Equal(ianaIPv6GlobalException1) || ip.Equal(ianaIPv6GlobalException2) || ip.Equal(ianaIPv6GlobalException3) || containsCIDR(ianaIPv6GlobalExceptions, ip)
}

func containsCIDR(networks []*net.IPNet, ip net.IP) bool {
	for _, network := range networks {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func mustCIDR(raw string) *net.IPNet {
	_, network, err := net.ParseCIDR(raw)
	if err != nil {
		panic(err)
	}
	return network
}

var (
	// IANA special-purpose registries, 2025-10-09: Globally Reachable=false
	// rows plus explicitly noted N/A/terminated conservative denies.
	ianaIPv4NonGlobal = []*net.IPNet{
		mustCIDR("0.0.0.0/8"),       // "this network"
		mustCIDR("100.64.0.0/10"),   // shared address space
		mustCIDR("192.0.0.0/24"),    // IETF protocol assignments
		mustCIDR("192.0.2.0/24"),    // documentation
		mustCIDR("192.88.99.0/24"),  // terminated 6to4 relay; conservatively deny
		mustCIDR("198.18.0.0/15"),   // benchmarking
		mustCIDR("198.51.100.0/24"), // documentation
		mustCIDR("203.0.113.0/24"),  // documentation
		mustCIDR("240.0.0.0/4"),     // reserved
	}
	ianaIPv4GlobalException1 = net.ParseIP("192.0.0.9")
	ianaIPv4GlobalException2 = net.ParseIP("192.0.0.10")
	// Well-known NAT64: classify from embedded IPv4. Custom NAT64 remains a
	// deployer egress-policy boundary; this does not claim to cover it.
	ianaNAT64WellKnown = mustCIDR("64:ff9b::/96")
	ianaIPv6IETF       = mustCIDR("2001::/23")
	ianaIPv6NonGlobal  = []*net.IPNet{
		mustCIDR("64:ff9b:1::/48"), // locally assigned NAT64
		mustCIDR("100::/64"),       // discard-only
		mustCIDR("100:0:0:1::/64"), // dummy IPv6 prefix
		mustCIDR("2001::/32"),      // Teredo: IANA N/A, conservatively deny
		mustCIDR("2001:db8::/32"),  // documentation
		mustCIDR("3fff::/20"),      // documentation
		mustCIDR("5f00::/16"),      // SRv6 SID
		mustCIDR("2002::/16"),      // 6to4: IANA N/A, conservatively deny
	}
	ianaIPv6GlobalException1 = net.ParseIP("2001:1::1")
	ianaIPv6GlobalException2 = net.ParseIP("2001:1::2")
	ianaIPv6GlobalException3 = net.ParseIP("2001:1::3")
	ianaIPv6GlobalExceptions = []*net.IPNet{
		mustCIDR("2001:3::/32"),     // AMT
		mustCIDR("2001:4:112::/48"), // AS112-v6
		mustCIDR("2001:20::/28"),    // ORCHIDv2
		mustCIDR("2001:30::/28"),    // DRIP
	}
)
