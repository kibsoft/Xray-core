package splithttp_test

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
)

func Test_GetNormalizedPath(t *testing.T) {
	tests := []struct {
		TestName           string
		Path               string
		SessionIDPlacement string
		SeqPlacement       string
		Expected           string
	}{
		{
			TestName: "default placement keeps trailing slash",
			Path:     "/sh",
			Expected: "/sh/",
		},
		{
			TestName: "query string is stripped",
			Path:     "/?world",
			Expected: "/",
		},
		{
			TestName:           "both off path drops trailing slash",
			Path:               "/stream",
			SessionIDPlacement: "query",
			SeqPlacement:       "query",
			Expected:           "/stream",
		},
		{
			TestName:           "both off path keeps file-like path",
			Path:               "/stream/filename.extension",
			SessionIDPlacement: "query",
			SeqPlacement:       "header",
			Expected:           "/stream/filename.extension",
		},
		{
			TestName:           "seq in path keeps trailing slash",
			Path:               "/stream",
			SessionIDPlacement: "query",
			Expected:           "/stream/",
		},
		{
			TestName:     "session in path keeps trailing slash",
			Path:         "/stream",
			SeqPlacement: "cookie",
			Expected:     "/stream/",
		},
		{
			TestName:           "existing trailing slash preserved",
			Path:               "/stream/",
			SessionIDPlacement: "query",
			SeqPlacement:       "query",
			Expected:           "/stream/",
		},
		{
			TestName:           "root unchanged",
			Path:               "/",
			SessionIDPlacement: "query",
			SeqPlacement:       "query",
			Expected:           "/",
		},
	}
	for _, test := range tests {
		t.Run(test.TestName, func(t *testing.T) {
			c := Config{
				Path:               test.Path,
				SessionIDPlacement: test.SessionIDPlacement,
				SeqPlacement:       test.SeqPlacement,
			}
			assert.Equal(t, test.Expected, c.GetNormalizedPath())
		})
	}
}

func Test_FillPacketRequest_GetBody(t *testing.T) {
	data := []byte("hello xray")
	payload := buf.MergeBytes(nil, data)

	req, err := http.NewRequest("POST", "https://example.com/", nil)
	common.Must(err)

	config := &Config{}
	config.FillPacketRequest(req, "sess", "0", payload)

	if req.GetBody == nil {
		t.Fatalf("Expected GetBody to be set")
	}

	first, err := io.ReadAll(req.Body)
	common.Must(err)

	if string(data) != string(first) {
		t.Fatalf("Body mismatch. Format %q and %q are not equal", data, first)
	}

	body2, err := req.GetBody()
	common.Must(err)

	second, err := io.ReadAll(body2)
	common.Must(err)

	if string(data) != string(second) {
		t.Fatalf("Replayed body mismatch. Format %q and %q are not equal", data, second)
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

func TestFileLikePathPreserved(t *testing.T) {
	for _, path := range []string{"/index.html", "/manuals/index.html"} {
		assert.Equal(t, path, (&Config{Path: path}).GetNormalizedPath())
	}
}
