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
		resolved, err := net.LookupIP(host)
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
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() {
		return false
	}
	// CGNAT 100.64.0.0/10 is neither IsPrivate nor link-local.
	if ip4 := ip.To4(); ip4 != nil && ip4[0] == 100 && ip4[1] >= 64 && ip4[1] < 128 {
		return false
	}
	return true
}
