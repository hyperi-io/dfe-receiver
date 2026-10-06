# Internet-Facing Hardening & Fronting Architecture

March 2026

**dfe-receiver is the internet-facing ingest component in the DFE stack. dfe-ui is the other component that takes untrusted input.**

This document covers application-level hardening (80/20 effort) and cost-effective infrastructure fronting for K8s and AWS deployments. It answers the questions cloud architects and security reviews ask most often.

---

## Current State (What We Already Have)

| Protection | Status | Config |
|---|---|---|
| Request body size limit (413) | 10 MiB (HTTP, OTLP HTTP, HEC, RW), 1 MiB (webhook); the body extractors read to the same limit | `server.max_body_size` (HTTP, OTLP HTTP), `splunk_hec.max_body_size`, `prometheus_rw.max_body_size`, `webhook.max_body_size` |
| Request timeout (408) | 30s, every HTTP listener | `server.request_timeout_ms` (HTTP, OTLP HTTP), `splunk_hec.request_timeout_ms`, `prometheus_rw.request_timeout_ms`, `webhook.request_timeout_ms` |
| Slowloris protection | 5s header_read_timeout, every HTTP listener (hyper) | `HEADER_READ_TIMEOUT` |
| Connection idle timeout | 60s, HTTP/1 keepalive + HTTP/2 | `CONNECTION_IDLE_TIMEOUT` |
| Concurrency limit | 10,000 in-flight requests default, per HTTP listener | `server.max_concurrent_requests` |
| Per-IP rate limiting | GCRA via tower-governor, opt-in, per HTTP listener, keyed on the TCP peer (forwarding headers only from `server.trusted_proxies`); clients whose budget has refilled are dropped from its key map every 5s | `server.rate_limit.*`, `server.trusted_proxies` |
| IP filter (allowlist/denylist) | CIDR trie, connection-level reject, every accept loop; an unknown mode, a bad CIDR or an empty allowlist refuses to start | `server.ip_filter.*` |
| 503 backpressure | HTTP + gRPC ingest shed load when pipeline not ready | `Retry-After: 5` |
| TLS termination | Per-protocol, hot-reloadable | `*.tls.enabled` |
| TLS handshake timeout | 10s hard-coded, all TCP handlers | `TLS_HANDSHAKE_TIMEOUT` |
| mTLS client auth | Per-protocol | `*.tls.client_auth: required` |
| Bearer token auth | Per-protocol, hot-reloadable | `*.auth.mode: bearer` |
| Header auth | HTTP, against `accepted_headers` only; an unknown `*.auth.mode` refuses to start | `server.auth.mode: header` |
| Credential-less listeners | Refuse to start without client certificates required or the opt-out | `*.accept_unauthenticated` |
| JSON validation | Global, always on: a body that is not JSON is refused with a 400 | Not configurable |
| Required field check | Global | `validation.required_fields` |
| Dead-letter queue | Global | `routing.dlq` |
| Memory pressure backpressure | 503 / `UNAVAILABLE` on HTTP and gRPC ingest | `DFE_RECEIVER_MEMORY_PRESSURE_THRESHOLD`, `self_regulation.*` (`buffer.*` bounds only the destination queues) |
| Middleware ordering | Concurrency -> Timeout -> BodyLimit -> Auth -> Handler | Correct for DoS |
| Frame size validation | GELF 1MB, Syslog 64KB, Fluent 32MB | Per-protocol limits |
| Zip-bomb rejection | Lumberjack nested compression rejected | Hard-coded |
| Health/readiness (K8s) | `/livez`, `/readyz`: 503 until every enabled listener serves, once one stops (drain included), and under memory pressure | Metrics port and the HTTP ingest port |
| Graceful shutdown | CancellationToken + in-flight drain | All handlers |
| Log spam prevention | Sampled (1/100) + debounced (5s) logging; a refused credential logs once per 5s per reason, an IP-filter refusal once per 5s per listener, and a counter carries every one | Per-protocol |
| `#![forbid(unsafe_code)]` | Entire crate | Cargo.toml lints |

---

## Part 1: Application-Level Hardening (Complete)

All planned application-level hardening is implemented. No remaining items.

### Which listener gets which control

`server.ip_filter` runs in the accept loop, so it reaches anything the receiver
accepts itself. `server.rate_limit` is a tower layer over an HTTP request, so it
reaches HTTP only. Each HTTP listener builds its own governor: the configured
rate is per source IP PER LISTENER, not a receiver-wide total.

`server.rate_limit.requests_per_second` is a rate: the limiter replenishes one
request of the quota every `1/requests_per_second` of a second, with
`server.rate_limit.burst` on top of it.

The limiter keys on the TCP peer; forwarding headers count only from
`server.trusted_proxies`. On Kubernetes, `externalTrafficPolicy: Cluster` (the
default) makes every client behind one node share its budget: set
`externalTrafficPolicy: Local`, or list the fronting proxy as trusted.

| Listener | `server.ip_filter` | `server.rate_limit` | Client authentication |
|---|---|---|---|
| HTTP `/ingest` | yes | yes | `server.auth` |
| Webhook (own or shared listener) | yes | yes | per-caller secret |
| Splunk HEC (TLS and plaintext) | yes | yes | `splunk_hec.auth` |
| Prometheus remote write | yes | yes | `prometheus_rw.auth` |
| OTLP HTTP (4318) | yes | yes | `otlp.auth` |
| OTLP gRPC (4317) | no -- tonic runs the accept loop | no -- no HTTP layer there | `otlp.auth`, bearer or mTLS |
| gRPC / Vector | no -- tonic runs the accept loop | no -- no HTTP layer there | `grpc.auth`, bearer or mTLS |
| Syslog UDP / TCP / TLS | yes (per datagram on UDP) | no -- no HTTP request to count | TLS listener only, `client_auth: required`; starts only with `syslog.accept_unauthenticated: true` |
| Lumberjack / Beats | yes | no -- no HTTP request to count | `lumberjack.tls.client_auth: required`, or `lumberjack.accept_unauthenticated: true` |
| Fluent Forward | yes | no -- no HTTP request to count | `fluent.tls.client_auth: required`, or `fluent.accept_unauthenticated: true` |
| GELF | yes | no -- no HTTP request to count | `gelf.tls.client_auth: required`, or `gelf.accept_unauthenticated: true` |
| Flow (NetFlow / sFlow) | own `flow.ip_filter` | own `flow.rate_limit` | none -- UDP, restrict by source |

An IP allowlist is network admission, not authentication: it says where a client
may connect from, not who the client is, which is why it is not in the last
column. The opt-out says in the config that anyone who can reach the port is
accepted, so pair it with an allowlist.

### Upgrade note: one IP filter, every listener

`server.ip_filter` used to reach `/ingest` and the webhook intake and nothing
else. It now runs in every accept loop the receiver owns, so a single allowlist
governs all of them.

A deployment that set an allowlist for its `/ingest` senders and receives syslog,
Beats, Fluent Forward, GELF, HEC, remote write or OTLP HTTP from a different
range starts DROPPING those events in the accept loop. Each drop counts on
`receiver_ip_filter_rejected_total{transport}`, and the filter logs one line
naming the listener at most every 5 seconds. Before upgrading, widen
`server.ip_filter.cidrs` to cover every sender on every enabled listener, or set
`mode: disabled` and restrict at the network edge.

---

## Part 2: K8s Fronting Architecture

When running in Kubernetes, the gateway/ingress layer handles most of what
Part 1 describes. This section covers how to configure it properly.

### 2.1 Ingress Controller Landscape (March 2026)

**The community `kubernetes/ingress-nginx` is EOL as of March 2026.**
It was announced at KubeCon NA 2025. No further CVE patches, no K8s
compatibility updates. Do not deploy it for new workloads.

The two NGINX ingress controllers are often confused:

| Project | Status (March 2026) |
|---|---|
| `kubernetes/ingress-nginx` (K8s SIG community) | **EOL. No CVE patches.** Chainguard maintains a security-only fork (EmeritOSS) as a stopgap. |
| `nginxinc/kubernetes-ingress` (F5) | Active. Apache 2.0. NGINX Plus features require JWT license. |
| Envoy Gateway | Active. CNCF project. Gateway API native. Recommended for new deployments. |

**Prefer Gateway API (Envoy-based) over Ingress for new deployments.**

### 2.2 Recommended: Envoy Gateway

[Envoy Gateway](https://gateway.envoyproxy.io/) is the CNCF reference
implementation of the Kubernetes Gateway API, built on Envoy Proxy.
Reached v1.2 (stable) -- production-ready for all use cases described here.
Open source, Apache 2.0.

**Future path:** If service mesh features are ever needed (mTLS between
services, traffic shifting, canary deployments), Istio ambient mesh
(sidecar-less, GA since Istio 1.22) uses Envoy as its data plane. The
Envoy Gateway investment carries over -- same proxy, same config patterns,
same operational knowledge.

**Security features via CRDs:**

**ClientTrafficPolicy** (downstream -- client-to-Envoy):

```yaml
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: ClientTrafficPolicy
metadata:
  name: dfe-receiver-client-policy
spec:
  targetRefs:
    - group: gateway.networking.k8s.io
      kind: Gateway
      name: dfe-gateway
  connection:
    # Connection limit -- closes new connections when exceeded
    connectionLimit:
      value: 10000
    # Buffer limit per connection
    bufferLimit: 16384
  timeout:
    http:
      # Slowloris protection -- reject if full request not received in 10s
      requestReceivedTimeout: 10s
```

**Note:** Envoy's native `request_headers_timeout` (the most precise
slowloris defence) is [not yet exposed](https://github.com/envoyproxy/gateway/issues/2598)
as a named field in ClientTrafficPolicy. `requestReceivedTimeout` covers
the full request (headers + body) which is the next best thing. For exact
header-only timeout, use `EnvoyPatchPolicy` to inject the raw Envoy HCM
`request_headers_timeout` setting.

**Rate limiting via BackendTrafficPolicy:**

```yaml
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: BackendTrafficPolicy
metadata:
  name: dfe-receiver-rate-limit
spec:
  targetRefs:
    - group: gateway.networking.k8s.io
      kind: HTTPRoute
      name: dfe-receiver-route
  rateLimit:
    type: Local
    local:
      rules:
        - limit:
            requests: 100
            unit: Second
```

Envoy Gateway supports both **local** (per-proxy-instance, no external deps)
and **global** (shared across replicas via external rate limit service with
Redis) rate limiting. For dfe-receiver, local rate limiting is sufficient
unless running many replicas where per-instance limits are too loose.

**HTTPRoute for dfe-receiver:**

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: dfe-receiver-route
spec:
  parentRefs:
    - name: dfe-gateway
  hostnames:
    - "ingest.example.com"
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /
      backendRefs:
        - name: dfe-receiver
          port: 8080
```

### 2.3 CrowdSec (Free Community IP Reputation)

[CrowdSec](https://github.com/crowdsecurity/crowdsec) is an open-source,
community-driven IP reputation engine. MIT-licensed, Go-based (60x faster
than fail2ban), with a community blocklist updated by thousands of nodes.

The free tier includes the community blocklist.

**Envoy Gateway integration:** A community [envoy-proxy-crowdsec-bouncer](https://github.com/kdwils/envoy-proxy-crowdsec-bouncer)
exists (Go, updated Feb 2026). It works as an Envoy `ext_authz` filter via
a SecurityPolicy:

```yaml
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: SecurityPolicy
metadata:
  name: crowdsec-policy
spec:
  targetRefs:
    - group: gateway.networking.k8s.io
      kind: HTTPRoute
      name: dfe-receiver-route
  extAuth:
    grpc:
      backendRef:
        name: crowdsec-bouncer
        port: 50051
```

**Maturity caveat:** The Envoy bouncer is community-maintained and explicitly
marked "not tested in production." For production, evaluate carefully.
CrowdSec also has mature bouncers for Traefik and nginx (if using F5's
NGINX Ingress Controller as a transitional option).

**Effectiveness:** The community blocklist blocks a significant portion of
known scanners, botnets, and credential stuffing sources before they reach
the application.

### 2.4 Separate Gateway Instances (Isolation)

Run a **dedicated Envoy Gateway** for dfe-receiver, separate from other
services. A DoS against the ingestion endpoint should not cascade to
dashboards, APIs, or other services.

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
  name: dfe-ingest
spec:
  controllerName: gateway.envoyproxy.io/dfe-ingest-controller

---
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: dfe-gateway
spec:
  gatewayClassName: dfe-ingest
  listeners:
    - name: https
      port: 443
      protocol: HTTPS
      tls:
        mode: Terminate
        certificateRefs:
          - name: ingest-tls
```

This costs only the extra resource allocation and prevents cross-service impact.

---

## Part 3: AWS Fronting Architecture (Cost-Conscious)

**The cardinal rule: cost scales with traffic volume.** At high ingestion
volume, per-GB and per-request charges compound. Every layer that touches
traffic must be evaluated on its cost per GB.

### 3.1 What Each Layer Is Billed On

This table names the billing unit, not a price. Prices differ by region and
change often: check the pricing pages linked under References before sizing a
deployment. The billing models below are as of March 2026.

| Service | Billed on | Notes |
|---|---|---|
| NLB | Per hour, plus capacity units for data processed | Cheaper per GB than ALB. Cross-zone traffic adds a per-GB charge each way. |
| ALB | Per hour, plus capacity units for data and requests | Higher capacity-unit rate than NLB. |
| AWS WAF | Per web ACL, per rule, per million requests | Bot Control adds a further ACL charge and request charges. |
| Shield Standard | Free | L3/L4 only, no L7. |
| Shield Advanced | Flat monthly fee | Includes WAF. 12-month commitment. |
| CloudFront (pay as you go) | Per GB out, per request, in volume tiers | Tiers cheapen with volume but stay the largest line at ingestion volume. |
| CloudFront (flat-rate plans) | Flat monthly fee with a traffic allowance | Performance degrades once the allowance is exceeded. |
| Cloudflare Free | Flat | Unlimited DDoS mitigation, 5 WAF rules. |
| Cloudflare Pro | Flat per domain | Managed WAF, 20 rules. Not billed per GB of HTTP traffic. |
| Cloudflare Spectrum (TCP) | Per GB, after a small free allowance | Prohibitive at ingestion volume. |
| CrowdSec | Free tier | Community blocklist, K8s native. |

### 3.2 Why CloudFront is Wrong for Ingestion

CloudFront is a CDN. It's designed to **serve** content, not **receive** it.

**Problems for data ingestion:**

- Pay-as-you-go pricing is per GB transferred -- brutal at ingestion volume
- Designed to cache and serve, not proxy POST requests to origin
- Adds latency (edge -> origin hop) for non-cacheable traffic
- The flat-rate plans degrade performance (fewer edge locations) when you
  exceed the allowance
- You're paying for CDN features (caching, edge compute) you don't use

**The one exception:** If you need AWS WAF (which only attaches to
CloudFront, ALB, or API Gateway), then CloudFront becomes a required
intermediary. But question whether you need AWS WAF at all (see 3.4).

### 3.3 Recommended AWS Architecture (Cost-Optimised)

```text
Internet
    |
    +-- [Cloudflare DNS + Proxy]  <- flat-rate plan, unlimited DDoS
    |     L3/L4/L7 DDoS mitigation
    |     WAF rules (free tier: 5, Pro: 20)
    |     IP reputation, bot mitigation
    |
    +-- [AWS NLB]  <- same-AZ preferred
          L4 load balancing
          Static IP (for Cloudflare origin)
          Health checks
          |
          +-- [K8s Service]
                |
                +-- [Envoy Gateway]  <- open source
                      Rate limiting (local or global)
                      Connection limits
                      Request timeout (slowloris)
                      CrowdSec ext_authz bouncer
                      |
                      +-- [dfe-receiver pods]
                            App-level auth (bearer/mTLS)
                            JSON validation
                            Body size limits
                            Concurrency limits
```

**Cost drivers:** the recurring charges are the Cloudflare plan (flat rate)
and the NLB (per hour plus data processed). Envoy Gateway and CrowdSec are
open source.

**Compare to the "just use AWS" approach** (ALB, AWS WAF, Shield Standard):
the bill is ALB hours and data, plus WAF per-ACL, per-rule and per-request
charges. It still lacks L7 DDoS mitigation, IP reputation and bot mitigation.

Cloudflare's HTTP plans are flat-rate rather than per GB or per request, so
the Cloudflare + NLB approach gets cheaper than ALB + WAF as volume grows AND
provides better protection.

### 3.4 When You DON'T Need AWS WAF

AWS WAF is the right choice when:

- Compliance requires AWS-native security controls
- You need deep integration with AWS services (API Gateway, AppSync)
- You're already on Shield Advanced, which includes WAF

AWS WAF is **overkill** when:

- Cloudflare (or similar) already handles L7 filtering upstream
- Your traffic is API/machine-to-machine (not browsers, less attack surface)
- You have rate limiting + auth at the application/gateway layer
- Cost is a major constraint

**For dfe-receiver specifically:** The traffic is structured JSON from known
agents (Vector, Beats, etc.), not browser traffic. The attack surface is
narrower than a typical web app. Cloudflare + Envoy Gateway rate limiting +
application auth covers the 80/20.

### 3.5 Architecture Variants

#### Variant A: Minimum Viable (Dev/Staging)

```text
Internet -> NLB -> K8s Service -> dfe-receiver
```

- Cost driver: the NLB base charge only
- Protection: Application-level only (auth, body limits, timeouts)
- Suitable for: Dev, staging, internal networks

#### Variant B: Production Standard

```text
Internet -> Cloudflare -> NLB -> Envoy Gateway -> dfe-receiver
```

- Cost driver: NLB data processing, plus a flat Cloudflare plan
- Protection: DDoS, WAF, rate limiting, IP reputation, auth
- Suitable for: Most production deployments

#### Variant C: High-Security / Compliance

```text
Internet -> CloudFront + AWS WAF -> ALB -> Envoy Gateway -> dfe-receiver
```

- Cost driver: CloudFront data transfer dominates
- Protection: Full AWS-native stack, compliance-ready
- Suitable for: Regulated industries, AWS-mandated security controls

#### Variant D: Maximum Protection

```text
Internet -> Cloudflare Enterprise -> NLB -> Envoy Gateway + CrowdSec -> dfe-receiver
```

- Cost driver: a Cloudflare Enterprise contract (custom pricing)
- Protection: Dedicated DDoS team, custom WAF rules, SLA
- Suitable for: Tier-1 production, SLA-bound deployments

### 3.6 Non-HTTP Protocols (Syslog, Beats, Fluent, GELF)

**Most cloud WAF/CDN services only handle HTTP/HTTPS traffic.** dfe-receiver's
TCP protocols need different treatment.

| Protocol | Port | Transport | Fronting Option |
|---|---|---|---|
| HTTP/HTTPS | 8080 | HTTP | Cloudflare + NLB/ALB (standard) |
| gRPC | 6000 | HTTP/2 | Cloudflare + NLB (gRPC passthrough) |
| OTLP gRPC | 4317 | HTTP/2 | Cloudflare + NLB |
| OTLP HTTP | 4318 | HTTP | Cloudflare + NLB/ALB |
| Splunk HEC | 8088 | HTTP | Cloudflare + NLB/ALB |
| Prometheus RW | 9091 | HTTP | Cloudflare + NLB/ALB |
| Lumberjack | 5044 | TCP | NLB only (no L7 proxy) |
| Syslog UDP | 514 | UDP | NLB only |
| Syslog TCP | 514 | TCP | NLB only |
| Syslog TLS | 6514 | TCP+TLS | NLB only |
| Fluent | 24224 | TCP | NLB only |
| GELF | 12201 | TCP | NLB only |

**For TCP protocols:**

- NLB handles L4 load balancing natively (TCP/UDP/TLS)
- No L7 inspection, no WAF, no rate limiting at this layer
- **Defence-in-depth:** Application-level auth + TLS + connection limits
  are the primary protections for these protocols
- Consider: mTLS for Beats/syslog/fluent clients (known agent fleet)
- Consider: IP allowlisting for TCP protocols (senders are usually known)

**Cloudflare Spectrum** proxies arbitrary TCP/UDP but is billed per GB after a
tiny free allowance. At ingestion volume, this is prohibitively expensive
and not recommended.

### 3.7 NLB Configuration Notes

**Same-AZ targeting:** To avoid the per-GB cross-zone data charge, configure
the NLB target group to use same-AZ routing. This requires your receiver
pods to be spread across AZs (which they should be for HA anyway).

```yaml
# Kubernetes Service annotation for NLB
apiVersion: v1
kind: Service
metadata:
  name: dfe-receiver
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-type: "external"
    service.beta.kubernetes.io/aws-load-balancer-nlb-target-type: "ip"
    service.beta.kubernetes.io/aws-load-balancer-scheme: "internet-facing"
    # Disable cross-zone to avoid the per-GB cross-zone data charge
    service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled: "false"
spec:
  type: LoadBalancer
  ports:
    - name: http
      port: 8080
      targetPort: 8080
      protocol: TCP
    - name: grpc
      port: 6000
      targetPort: 6000
      protocol: TCP
    - name: beats
      port: 5044
      targetPort: 5044
      protocol: TCP
    # ... additional ports
```

---

## Part 4: Defence-in-Depth Summary

The complete protection stack, from outer to inner:

```text
Layer 0: DNS          Cloudflare DNS (proxy mode) -- free DDoS + WAF
Layer 1: Cloud LB     AWS NLB -- L4 load balancing, health checks
Layer 2: K8s Gateway  Envoy Gateway -- rate limit, conn limit, request timeout
Layer 3: Community    CrowdSec -- IP reputation, community blocklist (via ext_authz)
Layer 4: Application  dfe-receiver -- auth, body limit, timeout, concurrency limit, 503 backpressure, slowloris protection, JSON validation
Layer 5: Transport    TLS/mTLS -- encryption, client verification
Layer 6: Kafka        Circuit breaker, backpressure -- downstream protection
```

**Each layer catches what the previous layer missed.**

No single layer is perfect. The value is in the combination:

- Cloudflare stops volumetric DDoS and known-bad IPs
- Envoy Gateway stops rate abuse, slow clients, connection floods
- CrowdSec stops emerging threats via community intelligence
- Application auth stops unauthorised access
- TLS/mTLS stops eavesdropping and impersonation
- Circuit breaker stops downstream cascade failures

---

### Infrastructure Fronting (Deployment-Time)

These are configured during deployment, not in application code:

| Layer | Component | When to Deploy |
|---|---|---|
| Envoy Gateway ClientTrafficPolicy | Connection limits + request timeout CRDs | Any K8s deployment |
| Envoy Gateway BackendTrafficPolicy | Rate limiting CRDs | Production K8s |
| Cloudflare DNS proxy | DNS config change | Internet-facing deployments |
| CrowdSec + Envoy ext_authz bouncer | Helm + SecurityPolicy CRDs | High-security deployments |

See Parts 2 and 3 above for configuration details and cost drivers.

---

## Fact-Check Log

Claims checked against the linked sources in March 2026:

| Claim | Verified | Source |
|---|---|---|
| `kubernetes/ingress-nginx` EOL March 2026 | Yes | [K8s blog Nov 2025](https://kubernetes.io/blog/2025/11/11/ingress-nginx-retirement/) |
| F5 NGINX Ingress Controller is Apache 2.0 | Yes | [NGINX blog](https://blog.nginx.org/blog/the-ingress-nginx-alternative-open-source-nginx-ingress-controller-for-the-long-term) |
| NGINX Plus requires JWT license (2024+) | Yes | [NGINX docs](https://docs.nginx.com/nginx-ingress-controller/) |
| freenginx fork (Feb 2024, Maxim Dounin) | Yes | [InfoQ](https://www.infoq.com/news/2024/03/freenginx-ngnix-web-server/) |
| Chainguard EmeritOSS fork (security-only) | Yes | [Fastly blog](https://www.fastly.com/blog/ingress-nginx-controller-kubernetes-retires-where-to-go-from-here) |
| Envoy Gateway ClientTrafficPolicy conn limit | Yes | [EG docs](https://gateway.envoyproxy.io/docs/tasks/traffic/connection-limit/) |
| `request_headers_timeout` not in ClientTrafficPolicy | Yes | [GH issue #2598](https://github.com/envoyproxy/gateway/issues/2598) |
| `requestReceivedTimeout` available | Yes | [EG docs](https://gateway.envoyproxy.io/latest/tasks/traffic/client-traffic-policy/) |
| tower-governor 0.6.0 supports axum 0.8 | Yes | [crates.io deps](https://crates.io/crates/tower_governor/0.6.0/dependencies) |
| CrowdSec Envoy bouncer exists | Yes | [GitHub](https://github.com/kdwils/envoy-proxy-crowdsec-bouncer) (updated Feb 2026) |
| CrowdSec Envoy bouncer is not production-tested | Yes | Author's own README |
| NLB is billed per hour plus data processed | Yes | [AWS ELB pricing](https://aws.amazon.com/elasticloadbalancing/pricing/) |
| NLB cross-zone traffic is billed per GB | Yes | [AWS blog](https://aws.amazon.com/blogs/networking-and-content-delivery/exploring-data-transfer-costs-for-aws-network-load-balancers/) |
| AWS WAF is billed per ACL, per rule and per request | Yes | [AWS WAF pricing](https://aws.amazon.com/waf/pricing/) |
| Shield Advanced is a flat monthly fee | Yes | [AWS WAF pricing](https://aws.amazon.com/waf/pricing/) |
| Cloudflare Free: unlimited DDoS + 5 WAF rules | Yes | [Cloudflare plans](https://www.cloudflare.com/plans/) |
| Cloudflare Pro includes a managed WAF | Yes | [Cloudflare Pro](https://www.cloudflare.com/plans/pro/) |
| Cloudflare Spectrum is billed per GB after a free allowance | Yes | [Cloudflare billing](https://support.cloudflare.com/hc/en-us/articles/360041721872-Billing-for-Spectrum) |
| CloudFront pay-as-you-go is billed per GB in volume tiers | Yes | [CloudFront pricing](https://aws.amazon.com/cloudfront/pricing/) |
| axum slowloris vulnerability | Yes | [GH issue #2741](https://github.com/tokio-rs/axum/issues/2741) |
| Envoy `request_headers_timeout` for slowloris | Yes | [Envoy docs](https://www.envoyproxy.io/docs/envoy/latest/faq/configuration/timeouts) |

---

## References

### Axum/Tower Security

- [axum Slowloris issue #2741](https://github.com/tokio-rs/axum/issues/2741)
- [axum idle connection timeout discussion #2938](https://github.com/tokio-rs/axum/discussions/2938)
- [axum connection limit discussion #2561](https://github.com/tokio-rs/axum/discussions/2561)
- [tower-governor 0.6 (rate limiting)](https://github.com/benwis/tower-governor)
- [axum-server-timeout example](https://github.com/josecelano/axum-server-timeout)

### K8s Ingress/Gateway Lifecycle

- [Ingress NGINX Retirement announcement (K8s blog)](https://kubernetes.io/blog/2025/11/11/ingress-nginx-retirement/)
- [F5 NGINX Ingress Controller (Apache 2.0)](https://blog.nginx.org/blog/the-ingress-nginx-alternative-open-source-nginx-ingress-controller-for-the-long-term)
- [Ingress NGINX EOL analysis](https://medium.com/@h.stoychev87/nginx-ingress-end-of-life-2026-f30e53e14a2e)
- [Fastly on Chainguard EmeritOSS fork](https://www.fastly.com/blog/ingress-nginx-controller-kubernetes-retires-where-to-go-from-here)

### Envoy Gateway

- [Envoy Gateway docs](https://gateway.envoyproxy.io/)
- [ClientTrafficPolicy](https://gateway.envoyproxy.io/latest/tasks/traffic/client-traffic-policy/)
- [Connection Limit](https://gateway.envoyproxy.io/docs/tasks/traffic/connection-limit/)
- [Rate Limiting concepts](https://gateway.envoyproxy.io/latest/concepts/rate-limiting/)
- [Global Rate Limit task](https://gateway.envoyproxy.io/docs/tasks/traffic/global-rate-limit/)
- [GH #2598 -- requestHeadersTimeout support](https://github.com/envoyproxy/gateway/issues/2598)
- [Envoy timeout configuration](https://www.envoyproxy.io/docs/envoy/latest/faq/configuration/timeouts)

### AWS Pricing (Verify Before Use)

- [AWS ELB pricing](https://aws.amazon.com/elasticloadbalancing/pricing/)
- [AWS WAF pricing](https://aws.amazon.com/waf/pricing/)
- [AWS CloudFront pricing](https://aws.amazon.com/cloudfront/pricing/)
- [NLB cross-zone data transfer costs](https://aws.amazon.com/blogs/networking-and-content-delivery/exploring-data-transfer-costs-for-aws-network-load-balancers/)
- [ALB vs NLB cost analysis](https://www.oreateai.com/blog/aws-alb-vs-nlb-navigating-the-pricing-maze-for-your-eks-workloads/dec6782f4606b47df064a6c245e1cd97)

### Cloudflare

- [Cloudflare plans](https://www.cloudflare.com/plans/)
- [Cloudflare Pro features](https://www.cloudflare.com/plans/pro/)
- [Cloudflare Spectrum pricing](https://support.cloudflare.com/hc/en-us/articles/360041721872-Billing-for-Spectrum)
- [Cloudflare vs AWS WAF comparison](https://inventivehq.com/blog/cloudflare-vs-aws-shield-vs-azure-ddos-vs-google-cloud-armor-web-security-comparison)

### Open Source Security

- [CrowdSec](https://github.com/crowdsecurity/crowdsec)
- [CrowdSec Envoy bouncer](https://github.com/kdwils/envoy-proxy-crowdsec-bouncer)
- [CrowdSec + Envoy blog post](https://blog.kyledev.co/posts/wring-a-crowdsec-envoy-proxy-bouncer/)
- [CrowdSec K8s protection blog](https://blog.kyledev.co/posts/protecting-internet-facing-apps/)
