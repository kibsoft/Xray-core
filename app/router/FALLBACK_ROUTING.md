# stickyRandom strategy and fallback routing

## stickyRandom

`stickyRandom` picks a random alive outbound from the balancer selector and keeps using it until observatory reports it dead, then switches to another random alive outbound.

When all primary outbounds are dead, the router **tries** to enable fallback routing mode. Fallback is enabled only if at least one fallback outbound is currently **confirmed alive**. If fallback outbounds are also dead (or have not been probed yet), routing stays in primary mode and the sticky balancer's `fallbackTag` is used (typically `blocked`).

```json
{
  "tag": "main-sticky",
  "selector": ["primary-"],
  "strategy": { "type": "stickyRandom" },
  "fallbackTag": "blocked"
}
```

Requires an observatory (`fallbackObservatory` when fallback routing is enabled, see below).

## Fallback routing mode

Fallback mode uses a separate rule set and a catch-all balancer:

| Field | Role |
|-------|------|
| `routing.rules` | Primary mode rules |
| `routing.fallbackRules` | Optional exceptions in fallback mode (any normal routing rule) |
| `routing.fallbackBalancerTag` | Catch-all balancer when no fallback rule matches |

Flow:

1. **Primary mode** — `rules` are evaluated; unmatched traffic goes to the primary sticky balancer.
2. When all primary outbounds are dead **and** a fallback outbound is alive — fallback mode is enabled and routing is retried.
3. **Fallback mode** — `fallbackRules` are evaluated first; if nothing matches, `fallbackBalancerTag` is used.
4. When at least one primary outbound is alive again — primary mode is restored automatically.
5. If primary and fallback outbounds are all dead — stay in (or return to) primary mode and use `fallbackTag` / `blocked`. Do not send user traffic to fallback nodes.

## fallbackObservatory

Use `fallbackObservatory` instead of `observatory` / `burstObservatory` when fallback routing is enabled. Only one observatory feature can be configured.

| Field | Role |
|-------|------|
| `probeInterval` | Idle probe in **primary** mode. `0` disables periodic probes. If set, only the current sticky outbound is pinged |
| `recoveryProbeInterval` | While in **fallback** mode, probe primary (mux) outbounds so recovery is noticed. Default `15s` |
| `probeOnError` | On real outbound errors, probe that node. Mux errors may sweep siblings and fallback; whitelist errors only re-check fallback nodes. Default `true` |
| `ignoreErrors` | Error kinds that must not start an error-driven probe. Empty / omitted = ignore none. Values: `internalError` (HTTP/2 `INTERNAL_ERROR` RST), `wsasend` (local Windows abort / WSAECONNABORTED) |
| `errorProbeCooldown` | Minimum time between error-driven probes. Default `3s` |
| `enableConcurrency` | Probe several outbounds in parallel when a full check runs |

- **Startup:** once after Xray start, wait ~1s then probe all primary and fallback outbounds. If an error-driven probe already holds the slot (TUN still up after an Xray restart), startup **waits** for that probe to finish and then still runs the full check; it does not skip. If all primary outbounds are dead and at least one fallback outbound is alive, fallback mode is enabled immediately (avoids waiting for dial retries on cold-start LTE). Observation status is cleared only after this startup probe actually ran and routing stays in **primary** mode, so later Wi-Fi→LTE error follow-up can sweep all primaries. If already in **fallback** mode, status is kept. Errors for the node currently being checked are absorbed. Other pending errors are revalidated against completed checks. Cooldown drops duplicate work instead of starting waiting goroutines. Errors from transports/requests older than a completed check or physical-network reset are ignored.
- Healthy primary: no pings, or one sticky heartbeat at `probeInterval`.
- Mux outbound error: probe that outbound. If it is dead, always probe remaining primaries (stale Wi-Fi `Alive` must not block a later Wi-Fi→LTE sweep). Probe fallback outbounds when whitelist health is still unknown and routing is not already in fallback.
- Whitelist / fallback outbound error: probe that fallback node only (and sibling fallbacks if it is dead). Do not wake mux recovery. If a primary is already confirmed alive, ignore leftover fallback errors (XHTTP `INTERNAL_ERROR` after leaving fallback).
- With `ignoreErrors: ["internalError", "wsasend"]`, HTTP/2 `INTERNAL_ERROR` and local `wsasend` aborts do not start a probe. Dial/timeout/refused still do. Empty list ignores nothing.
- **Inflight / burst gate** (per outbound tag): `EOF` and a clean finish count as success. `context.Canceled`, closed-pipe, and local loopback SOCKS resets (`wsarecv` / forcibly closed on `127.0.0.1`) are noise, not death. Noise probes only when this tag has no other in-flight session, no success in the last 2s, and at least 3 noise errors in that window. Hard failures (`connectex`, timeout, refused, reset from a public remote) still probe immediately. Sibling tags do not share inflight. Observatory probes do not count as user sessions.
- Fallback mode: primary subjects on `recoveryProbeInterval`; fallback subjects on first enter (unknown health) / on their errors. Mux recovery ticks do not re-ping **alive** whitelist nodes. Fallback tags already marked dead are re-probed on the same interval so they can return to the balancer if they recover.
- **Leave fallback (primary recovered):** observation status is cleared (same as startup primary-mode clear) so a later Wi-Fi→LTE switch is not blocked by stale `Alive=true` from recovery probes.
- **Leave fallback (both dead):** status is kept. Clearing would treat unprobed primaries as alive and delay the next fallback entry after sticky fails over.
- Observatory probes do not count as outbound errors (they do not retrigger probing).

## Network canary (optional)

Add `networkCanary` to `fallbackObservatory` to tell a whitelist (WL) network from an open one **without pinging proxy servers**. Probes are TLS handshakes through the `direct` outbound (IP literals recommended, no DNS dependency).

| Field | Role |
|-------|------|
| `outboundTag` | Outbound used for canary probes, default `direct` |
| `foreign` | Foreign `host:port` targets (default port 443). Any success means the network is **open** |
| `domestic` | Domestic targets. Used only to tell WL from offline |
| `timeout` | Per-round timeout, default `2.5s` |
| `confirmRounds` | A non-open verdict must hold this many rounds, default `2` |
| `pollInterval` | In fallback mode: canary polling period, default `8s` |
| `minFallbackDwell` | Minimum time in fallback before leaving is considered, default `20s` |

Verdicts: `open` (foreign reachable), `whitelist` (foreign unreachable, domestic reachable), `offline` (nothing reachable). The canary is enabled only when both `foreign` and `domestic` are non-empty.

- With canary enabled, routing mode follows the network verdict independently of primary health. `whitelist` plus a confirmed alive fallback node enables WL; stale primary `Alive` does not prevent entry. `open` forbids WL even if every primary is dead.
- Primary error on an open network: one retry of that node, then it is marked dead and sticky fails over. No sibling sweep, no whitelist pings. Known-dead primaries are re-probed every 60s while the network is open.
- Primary error on `whitelist`: only unknown/stale whitelist nodes are probed so fallback can start. There is no speculative sweep while canary is still running.
- `offline`: nothing is marked dead or pinged.
- In fallback mode healthy WL servers are not polled. Unknown/dead WL nodes can be checked while `whitelist`; `offline` runs no server checks. When `open` returns, check the default primary pool once and pin its first confirmed healthy node. Keep these health results until the next physical network change; direct Internet reachability alone does not prove proxy reachability.
- Startup on `open` probes no servers; startup on `whitelist` probes only WL nodes.
- Embedders can call `NotifyNetworkChanged()` on the observer (exported through libXray). Notifications are coalesced, cancel old-network checks, invalidate cached canary/health and bypass traffic-error cooldowns. Results from older network generations are discarded. Offline radio bring-up gets two direct-canary retries. Android watches non-VPN physical networks, including unvalidated networks (a real WL network can fail Android's validation).
- WL routing starts as soon as the first node is confirmed alive; unprobed siblings remain ineligible until their probes succeed. Late errors from known-dead primaries do not re-probe them before the recovery timer.

## Example config

```json
{
  "fallbackObservatory": {
    "subjectSelector": ["primary-"],
    "fallbackSubjectSelector": ["fallback-"],
    "probeURL": "http://cp.cloudflare.com/generate_204",
    "probeInterval": "0",
    "recoveryProbeInterval": "15s",
    "probeOnError": true,
    "ignoreErrors": ["internalError", "wsasend"],
    "errorProbeCooldown": "3s",
    "enableConcurrency": true
  },
  "routing": {
    "fallbackBalancerTag": "fallback-random",
    "rules": [
      {
        "ruleTag": "ru-direct",
        "domain": ["geosite:category-ru"],
        "outboundTag": "direct"
      },
      {
        "ruleTag": "default",
        "network": "tcp,udp",
        "balancerTag": "main-sticky"
      }
    ],
    "fallbackRules": [
      {
        "ruleTag": "fb-ru-limited",
        "domain": ["bank.ru", "gosuslugi.ru"],
        "outboundTag": "direct"
      }
    ],
    "balancers": [
      {
        "tag": "main-sticky",
        "selector": ["primary-"],
        "strategy": { "type": "stickyRandom" },
        "fallbackTag": "blocked"
      },
      {
        "tag": "fallback-random",
        "selector": ["fallback-"],
        "strategy": { "type": "roundRobin" },
        "fallbackTag": "blocked"
      }
    ]
  }
}
```

**Primary mode:** `yandex.ru` → DIRECT, `google.com` → `main-sticky`.

**Fallback mode:** `bank.ru` → DIRECT (matched `fallbackRules`), `yandex.ru` → `fallback-random`, `google.com` → `fallback-random`.

Give both balancers a `fallbackTag` so empty picks go to `blocked` instead of a routing error.

## Runtime API

gRPC (`RoutingService`):

- `AddFallbackRule` — add or replace fallback rules
- `RemoveFallbackRule` — remove by `ruleTag`
- `GetRoutingMode` — returns `{ "fallbackMode": true|false }`

CLI (requires API inbound):

```bash
xray api adfbrules [--append] config.json
xray api rmfbrules ruleTag ...
xray api routingmode
```

## Limitations

- Sticky outbound choice is in-memory; restart picks a new random outbound.
- Only one `fallbackBalancerTag` and one sticky balancer (first `stickyRandom` balancer) are supported for recovery.
- `observatory` / `burstObservatory` cannot be used together with fallback routing.
- Same-request failover after an outbound `Process` failure is not performed; the next connection uses the updated mode.

## Passive stall detection

An observed TCP connection request or outgoing user payload arms a 3s watchdog. Any response from that outbound cancels it. A stalled request checks only its node (with the network canary enabled); a confirmed failure retires that node's mux workers and lets the sticky balancer pick another node. Without new connection requests or outgoing payload there are no periodic watchdog checks. Closing the last request cancels its timer; internal probes never arm it. A healthy confirmation with no real traffic progress backs off further stall checks (15s, 30s, up to 60s); real downlink progress restores fast detection. The check itself retains its 5s timeout. Vision downlink for observed connections uses buffered copy so progress reaches the watchdog; unrelated connections retain kernel splice. Retiring a mux worker also cancels its in-progress handshake. Confirmed node failure and physical-network reset also cancel RAW/Reality requests and interrupt their user streams, allowing immediate application retry rather than waiting for the old handshake. Session completion and progress from an old network cannot clear a new network's watchdog.

Android resets old transports on physical-network changes and on resume after at least 30s with the screen off or after Doze. Resume also renews the VPN network agent after routing is ready, even if the physical network handle is unchanged. This uses OS events, without a recurring server heartbeat.
