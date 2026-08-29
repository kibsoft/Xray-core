package utils

import (
	"net/http"
	"strings"
	"testing"
)

func TestPickAutoFirefoxIdentityLooksLikeFirefox(t *testing.T) {
	ua, lang := PickAutoFirefoxIdentity()
	if !strings.Contains(ua, "Firefox/") || !strings.Contains(ua, "Gecko/20100101") {
		t.Fatalf("unexpected UA: %s", ua)
	}
	if lang == "" {
		t.Fatal("empty Accept-Language")
	}
}

func TestStickyAutoHeadersStable(t *testing.T) {
	h1 := http.Header{}
	h1.Set("User-Agent", "auto")
	h1.Set("Accept", "application/pdf,*/*;q=0.8")
	TryDefaultHeadersWith(h1, "fetch")
	h2 := http.Header{}
	h2.Set("User-Agent", "auto")
	TryDefaultHeadersWith(h2, "fetch")
	if h1.Get("User-Agent") != h2.Get("User-Agent") {
		t.Fatalf("UA should be sticky in-process: %q vs %q", h1.Get("User-Agent"), h2.Get("User-Agent"))
	}
	if h1.Get("Accept") != "application/pdf,*/*;q=0.8" {
		t.Fatalf("Accept should be kept: %q", h1.Get("Accept"))
	}
	if h1.Get("Accept-Language") == "" {
		t.Fatal("empty Accept-Language")
	}
}
