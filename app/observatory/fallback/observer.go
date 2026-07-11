package fallback

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	v2net "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/signal/done"
	"github.com/xtls/xray-core/common/task"
	"github.com/xtls/xray-core/common/utils"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/internet/tagged"
	"google.golang.org/protobuf/proto"
)

type Observer struct {
	config *Config
	ctx    context.Context

	statusLock sync.Mutex
	status     []*observatory.OutboundStatus

	finished *done.Instance

	ohm        outbound.Manager
	dispatcher routing.Dispatcher
	modeCtrl   routing.FallbackModeController
}

func (o *Observer) ProbeIntervalDuration() time.Duration {
	if o.config != nil && o.config.ProbeInterval != 0 {
		return time.Duration(o.config.ProbeInterval)
	}
	return 30 * time.Second
}

func (o *Observer) GetObservation(ctx context.Context) (proto.Message, error) {
	o.statusLock.Lock()
	defer o.statusLock.Unlock()
	return &observatory.ObservationResult{Status: o.status}, nil
}

func (o *Observer) Type() interface{} {
	return extension.ObservatoryType()
}

func (o *Observer) Start() error {
	if o.config != nil && len(o.config.SubjectSelector) != 0 {
		o.finished = done.New()
		go o.background()
	}
	return nil
}

func (o *Observer) Close() error {
	if o.finished != nil {
		return o.finished.Close()
	}
	return nil
}

func (o *Observer) ProbeFallback() {
	outbounds := o.fallbackOutboundTags()
	if len(outbounds) == 0 {
		return
	}
	o.probeOutbounds(outbounds)
}

func (o *Observer) fallbackOutboundTags() []string {
	if o.config == nil || len(o.config.FallbackSubjectSelector) == 0 {
		return nil
	}
	hs, ok := o.ohm.(outbound.HandlerSelector)
	if !ok {
		return nil
	}
	return hs.Select(o.config.FallbackSubjectSelector)
}

func (o *Observer) activeSelectors() []string {
	if o.config == nil {
		return nil
	}
	sels := append([]string{}, o.config.SubjectSelector...)
	if o.modeCtrl != nil && o.modeCtrl.IsFallbackMode() {
		sels = append(sels, o.config.FallbackSubjectSelector...)
	}
	return sels
}

func (o *Observer) background() {
	for !o.finished.Done() {
		hs, ok := o.ohm.(outbound.HandlerSelector)
		if !ok {
			errors.LogInfo(o.ctx, "outbound.Manager is not a HandlerSelector")
			return
		}

		outbounds := hs.Select(o.activeSelectors())
		o.clearRemovedOutbounds(outbounds)

		sleepTime := time.Second * 10
		if o.config.ProbeInterval != 0 {
			sleepTime = time.Duration(o.config.ProbeInterval)
		}

		if !o.config.EnableConcurrency {
			sort.Strings(outbounds)
			for _, v := range outbounds {
				result := o.probe(v)
				o.updateStatusForResult(v, &result)
				if o.finished.Done() {
					return
				}
				time.Sleep(sleepTime)
			}
			continue
		}

		o.probeOutbounds(outbounds)
		time.Sleep(sleepTime)
	}
}

func (o *Observer) probeOutbounds(outbounds []string) {
	ch := make(chan struct{}, len(outbounds))
	for _, v := range outbounds {
		go func(v string) {
			result := o.probe(v)
			o.updateStatusForResult(v, &result)
			ch <- struct{}{}
		}(v)
	}
	for range outbounds {
		if o.finished == nil {
			<-ch
			continue
		}
		select {
		case <-ch:
		case <-o.finished.Wait():
			return
		}
	}
}

func (o *Observer) clearRemovedOutbounds(outbounds []string) {
	o.statusLock.Lock()
	defer o.statusLock.Unlock()
	if len(o.status) == 0 {
		return
	}
	var pruned []*observatory.OutboundStatus
	for _, status := range o.status {
		if slices.Contains(outbounds, status.OutboundTag) {
			pruned = append(pruned, status)
		}
	}
	o.status = pruned
}

func (o *Observer) probe(outboundTag string) observatory.ProbeResult {
	errorCollectorForRequest := newErrorCollector()

	httpTransport := http.Transport{
		Proxy: func(*http.Request) (*url.URL, error) {
			return nil, nil
		},
		DialContext: func(ctx context.Context, network string, addr string) (net.Conn, error) {
			var connection net.Conn
			taskErr := task.Run(ctx, func() error {
				dest, err := v2net.ParseDestination(network + ":" + addr)
				if err != nil {
					return errors.New("cannot understand address").Base(err)
				}
				trackedCtx := session.TrackedConnectionError(o.ctx, errorCollectorForRequest)
				conn, err := tagged.Dialer(trackedCtx, o.dispatcher, dest, outboundTag)
				if err != nil {
					return errors.New("cannot dial remote address ", dest).Base(err)
				}
				connection = conn
				return nil
			})
			if taskErr != nil {
				return nil, errors.New("cannot finish connection").Base(taskErr)
			}
			return connection, nil
		},
		TLSHandshakeTimeout: time.Second * 5,
	}
	httpClient := &http.Client{
		Transport: &httpTransport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Jar:     nil,
		Timeout: time.Second * 5,
	}
	var GETTime time.Duration
	err := task.Run(o.ctx, func() error {
		startTime := time.Now()
		probeURL := "https://www.google.com/generate_204"
		if o.config.ProbeUrl != "" {
			probeURL = o.config.ProbeUrl
		}
		req, _ := http.NewRequest(http.MethodGet, probeURL, nil)
		utils.TryDefaultHeadersWith(req.Header, "nav")
		response, err := httpClient.Do(req)
		if err != nil {
			return errors.New("outbound failed to relay connection").Base(err)
		}
		if response.Body != nil {
			response.Body.Close()
		}
		endTime := time.Now()
		GETTime = endTime.Sub(startTime)
		return nil
	})
	if err != nil {
		errorMessage := "the outbound " + outboundTag + " is dead: GET request failed:" + err.Error() + "with outbound handler report underlying connection failed"
		errors.LogInfoInner(o.ctx, errorCollectorForRequest.UnderlyingError(), errorMessage)
		return observatory.ProbeResult{Alive: false, LastErrorReason: errorMessage}
	}
	errors.LogInfo(o.ctx, "the outbound ", outboundTag, " is alive:", GETTime.Seconds())
	return observatory.ProbeResult{Alive: true, Delay: GETTime.Milliseconds()}
}

func (o *Observer) updateStatusForResult(outboundTag string, result *observatory.ProbeResult) {
	o.statusLock.Lock()
	defer o.statusLock.Unlock()
	var status *observatory.OutboundStatus
	if location := o.findStatusLocationLockHolderOnly(outboundTag); location != -1 {
		status = o.status[location]
	} else {
		status = &observatory.OutboundStatus{}
		o.status = append(o.status, status)
	}

	status.LastTryTime = time.Now().Unix()
	status.OutboundTag = outboundTag
	status.Alive = result.Alive
	if result.Alive {
		status.Delay = result.Delay
		status.LastSeenTime = status.LastTryTime
		status.LastErrorReason = ""
	} else {
		status.LastErrorReason = result.LastErrorReason
		status.Delay = 99999999
	}
}

func (o *Observer) findStatusLocationLockHolderOnly(outboundTag string) int {
	for i, v := range o.status {
		if v.OutboundTag == outboundTag {
			return i
		}
	}
	return -1
}

func New(ctx context.Context, config *Config) (*Observer, error) {
	var outboundManager outbound.Manager
	var dispatcher routing.Dispatcher
	err := core.RequireFeatures(ctx, func(om outbound.Manager, rd routing.Dispatcher) {
		outboundManager = om
		dispatcher = rd
	})
	if err != nil {
		return nil, errors.New("Cannot get depended features").Base(err)
	}
	observer := &Observer{
		config:     config,
		ctx:        ctx,
		ohm:        outboundManager,
		dispatcher: dispatcher,
	}
	core.OptionalFeatures(ctx, func(router routing.Router) {
		if ctrl, ok := router.(routing.FallbackModeController); ok {
			observer.modeCtrl = ctrl
		}
	})
	return observer, nil
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		return New(ctx, config.(*Config))
	}))
}

type errorCollector struct {
	errors *errors.Error
}

func (e *errorCollector) SubmitError(err error) {
	if e.errors == nil {
		e.errors = errors.New("underlying connection error").Base(err)
		return
	}
	e.errors = e.errors.Base(errors.New("underlying connection error").Base(err))
}

func newErrorCollector() *errorCollector {
	return &errorCollector{}
}

func (e *errorCollector) UnderlyingError() error {
	if e.errors == nil {
		return errors.New("failed to produce report")
	}
	return e.errors
}
