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

func Test_GetNormalizedQuery(t *testing.T) {
	c := Config{Path: "/index.html?static=1"}
	if got := c.GetNormalizedQuery(); got != "static=1" {
		t.Errorf("GetNormalizedQuery() = %q, want %q", got, "static=1")
	}
}
