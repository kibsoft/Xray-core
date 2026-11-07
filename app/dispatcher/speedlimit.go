package dispatcher

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/protocol"
)

const (
	kbpsToBytesPerSec = 1000.0 / 8.0
	minBurstBytes     = 16 * 1024
	maxBurstBytes     = 256 * 1024
)

// SpeedLimiter paces per-user uplink/downlink after authentication.
type SpeedLimiter struct {
	enabled         bool
	defaultDownKbps uint32
	defaultUpKbps   uint32
	unlimited       map[string]struct{}
	overrides       map[string]*UserRate

	mu     sync.Mutex
	pacers map[string]*userPacer
}

type userPacer struct {
	down *byteBucket
	up   *byteBucket
	refs atomic.Int32
}

type byteBucket struct {
	rate   float64 // bytes per second
	burst  float64
	mu     sync.Mutex
	tokens float64
	last   time.Time
}

// NewSpeedLimiter builds a limiter from dispatcher config. Nil/disabled config is a no-op limiter.
func NewSpeedLimiter(cfg *SpeedLimit) *SpeedLimiter {
	s := &SpeedLimiter{
		pacers:    make(map[string]*userPacer),
		unlimited: make(map[string]struct{}),
		overrides: make(map[string]*UserRate),
	}
	if cfg == nil || !cfg.GetEnabled() {
		return s
	}
	s.enabled = true
	s.defaultDownKbps = cfg.GetDefaultDownKbps()
	s.defaultUpKbps = cfg.GetDefaultUpKbps()
	for _, id := range cfg.GetUnlimited() {
		if key := normUserKey(id); key != "" {
			s.unlimited[key] = struct{}{}
		}
	}
	for id, rate := range cfg.GetOverrides() {
		if key := normUserKey(id); key != "" && rate != nil {
			s.overrides[key] = rate
		}
	}
	return s
}

func normUserKey(id string) string {
	return strings.ToLower(strings.TrimSpace(id))
}

func (s *SpeedLimiter) Enabled() bool {
	return s != nil && s.enabled
}

// Bind returns downlink and uplink buckets for this connection. Nil bucket means unlimited.
func (s *SpeedLimiter) Bind(ctx context.Context, user *protocol.MemoryUser) (down, up *byteBucket) {
	if !s.Enabled() || user == nil {
		return nil, nil
	}
	ids := user.Identifiers()
	if len(ids) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		if key := normUserKey(id); key != "" {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return nil, nil
	}
	for _, key := range keys {
		if _, ok := s.unlimited[key]; ok {
			return nil, nil
		}
	}

	downKbps, upKbps := s.defaultDownKbps, s.defaultUpKbps
	for _, key := range keys {
		if rate, ok := s.overrides[key]; ok && rate != nil {
			downKbps, upKbps = rate.GetDownKbps(), rate.GetUpKbps()
			break
		}
	}
	if downKbps == 0 && upKbps == 0 {
		return nil, nil
	}

	pacer := s.getOrCreate(keys, downKbps, upKbps)
	pacer.refs.Add(1)
	context.AfterFunc(ctx, func() {
		if pacer.refs.Add(-1) == 0 {
			s.drop(keys, pacer)
		}
	})
	return pacer.down, pacer.up
}

func (s *SpeedLimiter) getOrCreate(keys []string, downKbps, upKbps uint32) *userPacer {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range keys {
		if existing := s.pacers[key]; existing != nil {
			for _, alias := range keys {
				s.pacers[alias] = existing
			}
			return existing
		}
	}
	p := &userPacer{
		down: newByteBucket(downKbps),
		up:   newByteBucket(upKbps),
	}
	for _, key := range keys {
		s.pacers[key] = p
	}
	return p
}

func (s *SpeedLimiter) drop(keys []string, p *userPacer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range keys {
		if s.pacers[key] == p {
			delete(s.pacers, key)
		}
	}
}

func newByteBucket(kbps uint32) *byteBucket {
	if kbps == 0 {
		return nil
	}
	rate := float64(kbps) * kbpsToBytesPerSec
	burst := rate
	if burst < minBurstBytes {
		burst = minBurstBytes
	}
	if burst > maxBurstBytes {
		burst = maxBurstBytes
	}
	return &byteBucket{
		rate:   rate,
		burst:  burst,
		tokens: 0,
	}
}

func (b *byteBucket) wait(n int) {
	if b == nil || n <= 0 {
		return
	}
	need := float64(n)
	b.mu.Lock()
	now := time.Now()
	if b.last.IsZero() {
		b.last = now
	} else {
		elapsed := now.Sub(b.last).Seconds()
		if elapsed > 0 {
			b.tokens += elapsed * b.rate
			if b.tokens > b.burst {
				b.tokens = b.burst
			}
			b.last = now
		}
	}
	var sleep time.Duration
	if b.tokens >= need {
		b.tokens -= need
	} else {
		deficit := need - b.tokens
		b.tokens = 0
		sleep = time.Duration(deficit / b.rate * float64(time.Second))
		b.last = now.Add(sleep)
	}
	b.mu.Unlock()
	if sleep > 0 {
		time.Sleep(sleep)
	}
}

// SpeedLimitWriter delays writes to enforce a per-user download cap.
type SpeedLimitWriter struct {
	Writer  buf.Writer
	limiter *byteBucket
}

func (w *SpeedLimitWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	if w.limiter != nil && !mb.IsEmpty() {
		w.limiter.wait(int(mb.Len()))
	}
	return w.Writer.WriteMultiBuffer(mb)
}

func (w *SpeedLimitWriter) Close() error {
	return common.Close(w.Writer)
}

func (w *SpeedLimitWriter) Interrupt() {
	common.Interrupt(w.Writer)
}

type speedLimitReader struct {
	reader  buf.TimeoutReader
	limiter *byteBucket
}

func (r *speedLimitReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	mb, err := r.reader.ReadMultiBuffer()
	if r.limiter != nil && !mb.IsEmpty() {
		r.limiter.wait(int(mb.Len()))
	}
	return mb, err
}

func (r *speedLimitReader) ReadMultiBufferTimeout(timeout time.Duration) (buf.MultiBuffer, error) {
	mb, err := r.reader.ReadMultiBufferTimeout(timeout)
	if r.limiter != nil && !mb.IsEmpty() {
		r.limiter.wait(int(mb.Len()))
	}
	return mb, err
}

func (r *speedLimitReader) Interrupt() {
	common.Interrupt(r.reader)
}

func wrapWriter(writer buf.Writer, limiter *byteBucket) buf.Writer {
	if limiter == nil || writer == nil {
		return writer
	}
	return &SpeedLimitWriter{Writer: writer, limiter: limiter}
}

func wrapReader(reader buf.Reader, limiter *byteBucket) buf.Reader {
	if limiter == nil || reader == nil {
		return reader
	}
	tr, ok := reader.(buf.TimeoutReader)
	if !ok {
		tr = &buf.TimeoutWrapperReader{Reader: reader}
	}
	return &speedLimitReader{reader: tr, limiter: limiter}
}

// WriterHasRateLimit reports whether writer (or a SizeStatWriter chain) paces traffic.
func WriterHasRateLimit(writer buf.Writer) bool {
	for writer != nil {
		switch w := writer.(type) {
		case *SpeedLimitWriter:
			return true
		case *SizeStatWriter:
			writer = w.Writer
		default:
			return false
		}
	}
	return false
}

// SpeedFrom returns the limiter when d is the default dispatcher.
func SpeedFrom(d interface{}) *SpeedLimiter {
	if dd, ok := d.(*DefaultDispatcher); ok {
		return dd.speed
	}
	return nil
}
