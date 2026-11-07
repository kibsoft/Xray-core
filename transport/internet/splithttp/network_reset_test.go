package splithttp

import "testing"

type resetTestConn struct{ closed bool }

func (c *resetTestConn) IsClosed() bool { return c.closed }
func (c *resetTestConn) Close() error   { c.closed = true; return nil }

func TestNetworkResetRetiresXHTTPPool(t *testing.T) {
	old := globalDialerMap
	t.Cleanup(func() { globalDialerMap = old })
	conn := &resetTestConn{}
	client := &XmuxClient{XmuxConn: conn}
	client.Running.Store(2)
	globalDialerMap = map[dialerConf]*XmuxManager{{}: {xmuxClients: []*XmuxClient{client}}}
	ResetConnections()
	if len(globalDialerMap) != 0 || !conn.closed || !client.NotUsed.Load() {
		t.Fatal("network change must retire and close the old pool even with active streams")
	}
	ResetConnections() // repeated notifications are safe
}
