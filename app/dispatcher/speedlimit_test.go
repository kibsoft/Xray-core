package dispatcher

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/protocol"
	"google.golang.org/protobuf/proto"
)

type stubAccount struct {
	id string
}

func (s stubAccount) Equals(protocol.Account) bool { return false }
func (s stubAccount) ToProto() proto.Message       { return nil }
func (s stubAccount) UserID() string               { return s.id }

type countingWriter struct {
	n int32
}

func (c *countingWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	c.n += mb.Len()
	buf.ReleaseMulti(mb)
	return nil
}

func testUser(email, uuid string) *protocol.MemoryUser {
	u := &protocol.MemoryUser{Email: email}
	if uuid != "" {
		u.Account = stubAccount{id: uuid}
	}
	return u
}

func writeBytes(t *testing.T, w buf.Writer, n int) {
	t.Helper()
	payload := make([]byte, n)
	common.Must(w.WriteMultiBuffer(buf.MergeBytes(nil, payload)))
}

func TestSpeedLimitWriterPaces(t *testing.T) {
	limiter := NewSpeedLimiter(&SpeedLimit{
		Enabled:         true,
		DefaultDownKbps: 256, // 32 KiB/s
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	down, _ := limiter.Bind(ctx, testUser("1", ""))
	counter := &countingWriter{}
	writer := wrapWriter(counter, down)

	start := time.Now()
	writeBytes(t, writer, 32*1024)
	elapsed := time.Since(start)
	if elapsed < 700*time.Millisecond {
		t.Fatalf("expected pacing around 1s, finished in %s", elapsed)
	}
	if elapsed > 2500*time.Millisecond {
		t.Fatalf("pacing too slow: %s", elapsed)
	}
	if counter.n != 32*1024 {
		t.Fatalf("wrote %d bytes", counter.n)
	}
}

func TestSpeedLimitDisabledDoesNotSleep(t *testing.T) {
	limiter := NewSpeedLimiter(&SpeedLimit{Enabled: false, DefaultDownKbps: 256})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	down, up := limiter.Bind(ctx, testUser("1", ""))
	if down != nil || up != nil {
		t.Fatal("disabled limiter should not bind buckets")
	}
}

func TestSpeedLimitUnlimitedDoesNotSleep(t *testing.T) {
	limiter := NewSpeedLimiter(&SpeedLimit{
		Enabled:         true,
		DefaultDownKbps: 256,
		Unlimited:       []string{"48d7bab1-0007-4564-a950-ce4f664b4316"},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	down, _ := limiter.Bind(ctx, testUser("9", "48D7BAB1-0007-4564-A950-CE4F664B4316"))
	if down != nil {
		t.Fatal("unlimited uuid should skip limiter")
	}
}

func TestSpeedLimitOverrideSharesBucket(t *testing.T) {
	limiter := NewSpeedLimiter(&SpeedLimit{
		Enabled:         true,
		DefaultDownKbps: 100000,
		Overrides: map[string]*UserRate{
			"2": {DownKbps: 256, UpKbps: 256},
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	downEmail, _ := limiter.Bind(ctx, testUser("2", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"))
	downUUID, _ := limiter.Bind(ctx, testUser("", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"))
	if downEmail == nil || downUUID == nil {
		t.Fatal("expected override buckets")
	}
	if downEmail != downUUID {
		t.Fatal("email and uuid must share one bucket")
	}

	counter := &countingWriter{}
	writer := wrapWriter(counter, downEmail)
	start := time.Now()
	writeBytes(t, writer, 16*1024)
	writeBytes(t, writer, 16*1024)
	elapsed := time.Since(start)
	if elapsed < 700*time.Millisecond {
		t.Fatalf("shared override should still pace, finished in %s", elapsed)
	}
}

func TestWriterHasRateLimit(t *testing.T) {
	inner := &SizeStatWriter{Writer: buf.Discard}
	if WriterHasRateLimit(inner) {
		t.Fatal("stats writer is not a rate limiter")
	}
	outer := &SpeedLimitWriter{Writer: inner}
	if !WriterHasRateLimit(outer) {
		t.Fatal("outer speed limit writer should be detected")
	}
	nested := &SizeStatWriter{Writer: outer}
	if !WriterHasRateLimit(nested) {
		t.Fatal("speed limit inside stats writer should be detected")
	}
}
