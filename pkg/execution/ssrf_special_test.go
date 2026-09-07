package execution

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
)

func TestPublicAddressClassifierIANAContract(t *testing.T) {
	if isPubliclyRoutable(nil) || isPubliclyRoutable(net.IP{1, 2}) {
		t.Fatal("nil or invalid address accepted")
	}
	cases := []struct {
		address string
		allow   bool
	}{
		{"8.8.8.8", true}, {"0.0.0.1", false}, {"100.64.0.1", false}, {"198.18.0.1", false}, {"198.51.100.1", false}, {"203.0.113.1", false}, {"240.0.0.1", false}, {"255.255.255.255", false}, {"192.88.99.1", false}, {"192.88.99.2", false},
		{"192.0.0.1", false}, {"192.0.0.9", true}, {"192.0.0.10", true}, {"192.0.2.1", false},
		{"::ffff:10.0.0.1", false}, {"::ffff:198.18.0.1", false}, {"::ffff:8.8.8.8", true}, {"2606:4700:4700::1111", true}, {"2001::1", false}, {"2001:2::1", false}, {"2001:1::1", true}, {"2001:1::2", true}, {"2001:1::3", true},
		{"2001:3::1", true}, {"2001:4:112::1", true}, {"2001:20::1", true}, {"2001:30::1", true}, {"100::1", false}, {"100:0:0:1::1", false}, {"2001:db8::1", false}, {"3fff::1", false}, {"5f00::1", false}, {"2002:c000:0201::1", false},
		{"64:ff9b::0808:0808", true}, {"64:ff9b::0a00:0001", false}, {"64:ff9b::7f00:0001", false}, {"64:ff9b::c000:0201", false}, {"64:ff9b:1::1", false},
	}
	for _, tc := range cases {
		ip := net.ParseIP(tc.address)
		if got := isPubliclyRoutable(ip); got != tc.allow {
			t.Errorf("%s public=%t, want %t", tc.address, got, tc.allow)
		}
	}
}

func TestPublicURLAndDialRecheckUseInjectedLookups(t *testing.T) {
	if err := validatePublicHTTPURL("https://example.test/hook", func(string) ([]net.IP, error) { return []net.IP{net.ParseIP("8.8.8.8")}, nil }); err != nil {
		t.Fatalf("bind public lookup: %v", err)
	}
	called := false
	client := newPublicHTTPClientWithDial(func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("198.18.0.1")}}, nil
	}, func(context.Context, string, string) (net.Conn, error) {
		called = true
		return nil, errors.New("must not dial")
	})
	dial := client.Transport.(*http.Transport).DialContext
	if _, err := dial(context.Background(), "tcp", "example.test:443"); err == nil || called || !strings.Contains(err.Error(), "non-public") {
		t.Fatalf("dial=%v called=%t", err, called)
	}
	gotAddress := ""
	publicClient := newPublicHTTPClientWithDial(func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}}, nil
	}, func(_ context.Context, _ string, address string) (net.Conn, error) {
		gotAddress = address
		return nil, errors.New("expected stub")
	})
	_, _ = publicClient.Transport.(*http.Transport).DialContext(context.Background(), "tcp", "example.test:443")
	if gotAddress != "8.8.8.8:443" {
		t.Fatalf("public dial address=%q", gotAddress)
	}
}

func TestPublicURLLiteralSpecialDoesNotLookup(t *testing.T) {
	if err := validatePublicHTTPURL("https://198.18.0.1/hook", func(string) ([]net.IP, error) { panic("literal must not resolve") }); err == nil {
		t.Fatal("literal special address accepted")
	}
}
