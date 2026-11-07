package fallback

import (
	"sync"

	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/common/errors"
)

// Check only the default primary pool on return from WL. A direct canary proves
// Internet availability, but cannot prove that the selected proxy is reachable.
func (o *Observer) recoverPrimaryPool() {
	if !o.recoverPrimaries.Load() || o.NetworkRoutingState() != "open" {
		return
	}
	controller, ok := o.modeCtrl.(interface {
		PrimaryRecoveryCandidates() []string
		RecoverPrimary(string)
	})
	if !ok {
		return
	}
	tags := controller.PrimaryRecoveryCandidates()
	generation := o.networkEpoch.Load()
	o.statusLock.Lock()
	for _, tag := range tags {
		if o.findStatusLocationLockHolderOnly(tag) == -1 {
			o.status = append(o.status, &observatory.OutboundStatus{OutboundTag: tag})
		}
	}
	o.statusLock.Unlock()
	errors.LogInfo(o.ctx, "primary recovery: checking default pool ", tags)
	var group sync.WaitGroup
	for _, tag := range tags {
		group.Go(func() {
			result := o.probe(tag)
			if generation != o.networkEpoch.Load() {
				return
			}
			o.updateStatusForResult(tag, &result)
			if result.Alive {
				if o.recoverPrimaries.CompareAndSwap(true, false) {
					controller.RecoverPrimary(tag)
				}
			} else if h := o.ohm.GetHandler(tag); h != nil {
				if resetter, ok := h.(interface{ ResetNetworkConnections() }); ok {
					resetter.ResetNetworkConnections()
				}
			}
		})
	}
	group.Wait()
}
