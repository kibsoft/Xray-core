# stickyRandom strategy and fallback routing

## stickyRandom

`stickyRandom` picks a random alive outbound from the balancer selector and keeps using it until observatory reports it dead, then switches to another random alive outbound.

When all primary outbounds are dead, the router enables **fallback routing mode** automatically.

```json
{
  "tag": "main-sticky",
  "selector": ["primary-"],
  "strategy": { "type": "stickyRandom" }
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
2. When all primary outbounds are dead — fallback mode is enabled and routing is retried.
3. **Fallback mode** — `fallbackRules` are evaluated first; if nothing matches, `fallbackBalancerTag` is used.
4. When at least one primary outbound is alive again — primary mode is restored automatically.

## fallbackObservatory

Use `fallbackObservatory` instead of `observatory` / `burstObservatory` when fallback routing is enabled. Only one observatory feature can be configured.

- Primary mode: probes `subjectSelector` only.
- Fallback mode: probes `subjectSelector` + `fallbackSubjectSelector`.

## Example config

```json
{
  "fallbackObservatory": {
    "subjectSelector": ["primary-"],
    "fallbackSubjectSelector": ["fallback-"],
    "probeURL": "http://cp.cloudflare.com/generate_204",
    "probeInterval": "30s"
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
        "strategy": { "type": "stickyRandom" }
      },
      {
        "tag": "fallback-random",
        "selector": ["fallback-"],
        "strategy": { "type": "random" },
        "fallbackTag": "direct"
      }
    ]
  }
}
```

**Primary mode:** `yandex.ru` → DIRECT, `google.com` → `main-sticky`.

**Fallback mode:** `bank.ru` → DIRECT (matched `fallbackRules`), `yandex.ru` → `fallback-random`, `google.com` → `fallback-random`.

For the fallback balancer, use `random` with `fallbackTag` for alive filtering — no changes to the `random` strategy are required.

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
