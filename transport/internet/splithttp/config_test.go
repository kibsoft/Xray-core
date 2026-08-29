package splithttp_test

import (
	"testing"

	. "github.com/xtls/xray-core/transport/internet/splithttp"
)

func Test_GetNormalizedPath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "root with query only", path: "/?world", want: "/"},
		{name: "html file", path: "/index.html", want: "/index.html"},
		{name: "html file with query", path: "/index.html?static=1", want: "/index.html"},
		{name: "nested html file", path: "/manuals/index.html", want: "/manuals/index.html"},
		{name: "directory path", path: "/manuals", want: "/manuals/"},
		{name: "short directory path", path: "/sh", want: "/sh/"},
		{name: "relative directory path", path: "sh", want: "/sh/"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := Config{Path: tc.path}
			if got := c.GetNormalizedPath(); got != tc.want {
				t.Errorf("GetNormalizedPath() = %q, want %q", got, tc.want)
			}
		})
	}
}

func Test_mixedPathRotation(t *testing.T) {
	encoded := EncodePathSpec([]string{
		"/manuals/account/workbook-week-planning.pdf",
		"/manuals/account/onboarding-pack.zip",
		"/manuals/lessons/module-01-inbox.mp4",
		"/manuals/lessons/workshop-bot.mp4",
	}, 10, 0)
	c := Config{Path: encoded}
	if got := c.GetNormalizedPath(); got != "/manuals/" {
		t.Errorf("common prefix = %q", got)
	}
	if got := c.GetRequestPath(0); got != "/manuals/account/workbook-week-planning.pdf" {
		t.Errorf("seq0 = %q", got)
	}
	if got := c.GetRequestPath(9); got != "/manuals/account/workbook-week-planning.pdf" {
		t.Errorf("same file until rotate = %q", got)
	}
	if got := c.GetRequestPath(10); got != "/manuals/account/onboarding-pack.zip" {
		t.Errorf("seq10 = %q", got)
	}
	if got := c.GetRequestPath(20); got != "/manuals/lessons/module-01-inbox.mp4" {
		t.Errorf("seq20 = %q", got)
	}
	if got := c.GetRequestPath(30); got != "/manuals/lessons/workshop-bot.mp4" {
		t.Errorf("seq30 = %q", got)
	}
	if got := c.GetRequestPath(40); got != "/manuals/account/workbook-week-planning.pdf" {
		t.Errorf("wrap = %q", got)
	}
	if got := c.GetRequestPath(0, 2); got != "/manuals/lessons/module-01-inbox.mp4" {
		t.Errorf("offset 2 = %q", got)
	}

	static := Config{Path: "/manuals/account/workbook-week-planning.pdf"}
	if got := static.GetRequestPath(12); got != static.GetNormalizedPath() {
		t.Errorf("static GetRequestPath should not rotate, got %q", got)
	}
}

func Test_GetNormalizedQuery(t *testing.T) {
	c := Config{Path: "/index.html?static=1"}
	if got := c.GetNormalizedQuery(); got != "static=1" {
		t.Errorf("GetNormalizedQuery() = %q, want %q", got, "static=1")
	}
}

func Test_decoyPathsInSpec(t *testing.T) {
	encoded := EncodePathSpec([]string{"/manuals/account/workbook-week-planning.pdf"}, 120, 8)
	c := Config{Path: encoded + "||d=/index.html,/guides.html,/account.html"}
	got := c.GetDecoyPaths()
	if len(got) != 3 || got[0] != "/index.html" || got[2] != "/account.html" {
		t.Fatalf("GetDecoyPaths() = %#v", got)
	}
	if c.GetRequestPath(0) != "/manuals/account/workbook-week-planning.pdf" {
		t.Fatalf("tunnel path should ignore decoy, got %q", c.GetRequestPath(0))
	}
}

