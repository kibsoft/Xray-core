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

- Healthy primary: no pings, or one sticky heartbeat at `probeInterval`.
- Mux outbound error: probe that outbound. If it is dead and no other primary is confirmed alive, probe remaining primaries. Probe fallback outbounds only when whitelist health is still unknown and routing is not already in fallback.
- Whitelist / fallback outbound error: probe that fallback node only (and sibling fallbacks if it is dead). Do not wake mux recovery. If a primary is already confirmed alive, ignore leftover fallback errors (XHTTP `INTERNAL_ERROR` after leaving fallback).
- With `ignoreErrors: ["internalError", "wsasend"]`, HTTP/2 `INTERNAL_ERROR` and local `wsasend` aborts do not start a probe. Dial/timeout/refused still do. Empty list ignores nothing.
- **Inflight / burst gate** (per outbound tag): `EOF` and a clean finish count as success. `context.Canceled`, closed-pipe, and local loopback SOCKS resets (`wsarecv` / forcibly closed on `127.0.0.1`) are noise, not death. Noise probes only when this tag has no other in-flight session, no success in the last 2s, and at least 3 noise errors in that window. Hard failures (`connectex`, timeout, refused, reset from a public remote) still probe immediately. Sibling tags do not share inflight. Observatory probes do not count as user sessions.
- Fallback mode: primary subjects on `recoveryProbeInterval`; fallback subjects on first enter (unknown health) / on their errors. Mux recovery ticks do not re-ping **alive** whitelist nodes. Fallback tags already marked dead are re-probed on the same interval so they can return to the balancer if they recover.
- Observatory probes do not count as outbound errors (they do not retrigger probing).

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
