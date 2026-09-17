# Internet-Facing Hardening & Fronting Architecture
March 2026  

**dfe-receiver is the only direct internet-facing component in the DFE stack.**  

This document covers application-level hardening (80/20 effort) and cost-effective infrastructure fronting for K8s and AWS deployments.  
Based on the July-September 2026 PB/day stress tests and DFE 2.1/2.0 customer deployments  
Answers for common customer deployment questions (cloud architects, security reviews)  
HyperI internal -> see infrastructure standards, PB scale patterns  

---

## Current State (What We Already Have)

| Protection | Status | Config |
|---|---|---|
| Request body size limit | 10 MiB (HTTP/HEC/RW), per-protocol | `server.max_body_size` |
| Request timeout (408) | 30s, HTTP/HEC/RW | `server.request_timeout_ms` |
| Slowloris protection | 5s header_read_timeout, every HTTP listener (hyper) | `HEADER_READ_TIMEOUT` |
| Connection idle timeout | 60s, HTTP/1 keepalive + HTTP/2 | `CONNECTION_IDLE_TIMEOUT` |
| Concurrency limit | 10,000 in-flight requests default, per HTTP listener | `server.max_concurrent_requests` |
| Per-IP rate limiting | GCRA via tower-governor, opt-in, per HTTP listener | `server.rate_limit.*` |
| IP filter (allowlist/denylist) | CIDR trie, connection-level reject, every accept loop | `server.ip_filter.*` |
| 503 backpressure | HTTP + gRPC ingest shed load when pipeline not ready | `Retry-After: 5` |
| TLS termination | Per-protocol, hot-reloadable | `*.tls.enabled` |
| TLS handshake timeout | 10s hard-coded, all TCP handlers | `TLS_HANDSHAKE_TIMEOUT` |
| mTLS client auth | Per-protocol | `*.tls.client_auth: required` |
| Bearer token auth | Per-protocol, hot-reloadable | `*.auth.mode: bearer` |
| Header auth | HTTP | `server.auth.mode: header` |
| JSON validation | Global | `validation.require_json` |
| Required field check | Global | `validation.required_fields` |
| Dead-letter queue | Global | `routing.dlq` |
| Memory pressure backpressure | Internal, 503 on all ingest endpoints | `buffer.pressure_threshold` |
| Middleware ordering | Concurrency -> Timeout -> BodyLimit -> Auth -> Handler | Correct for DoS |
| Frame size validation | GELF 1MB, Syslog 64KB, Fluent 32MB | Per-protocol limits |
| Zip-bomb rejection | Lumberjack nested compression rejected | Hard-coded |
| Health/readiness (K8s) | `/livez`, `/readyz`, 503 on drain | All HTTP handlers |
| Graceful shutdown | CancellationToken + in-flight drain | All handlers |
| Log spam prevention | Sampled (1/100) + debounced (5s) logging | Per-protocol |
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

| Listener | `server.ip_filter` | `server.rate_limit` | Client authentication |
|---|---|---|---|
| HTTP `/ingest` | yes | yes | `server.auth` |
| Webhook (own or shared listener) | yes | yes | per-caller secret |
| Splunk HEC (TLS and plaintext) | yes | yes | `splunk_hec.auth` |
| Prometheus remote write | yes | yes | `prometheus_rw.auth` |
| OTLP HTTP (4318) | yes | yes | `otlp.auth` |
| OTLP gRPC (4317) | no -- tonic runs the accept loop | no -- no HTTP layer there | `otlp.auth`, bearer or mTLS |
| gRPC / Vector | no -- tonic runs the accept loop | no -- no HTTP layer there | `grpc.auth`, bearer or mTLS |
| Syslog UDP / TCP / TLS | yes (per datagram on UDP) | no -- no HTTP request to count | TLS listener only, `client_auth: required` |
| Lumberjack / Beats | yes | no -- no HTTP request to count | `lumberjack.tls.client_auth: required` |
| Fluent Forward | yes | no -- no HTTP request to count | none -- the Forward frames carry no credential |
| GELF | yes | no -- no HTTP request to count | none -- GELF has no in-protocol authentication |
| Flow (NetFlow / sFlow) | own `flow.ip_filter` | own `flow.rate_limit` | none -- UDP, restrict by source |

An IP allowlist is network admission, not authentication: it says where a client
may connect from, not who the client is, which is why it is not in the last
column. Fluent Forward and GELF have nothing in that column at all, so close
those ports with `tls.client_auth: required`, an allowlist, or both -- an
allowlist alone admits anything inside the range.

### Upgrade note: one IP filter, every listener

`server.ip_filter` used to reach `/ingest` and the webhook intake and nothing
else. It now runs in every accept loop the receiver owns, so a single allowlist
governs all of them.

A deployment that set an allowlist for its `/ingest` senders and receives syslog,
Beats, Fluent Forward, GELF, HEC, remote write or OTLP HTTP from a different
range starts DROPPING those events in the accept loop. The drop is a `debug!`
line and nothing else. Before upgrading, widen `server.ip_filter.cidrs` to cover
every sender on every enabled listener, or set `mode: disabled` and restrict at
the network edge.

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

**Per HyperI K8s standards: prefer Gateway API (Envoy-based) over Ingress.**

### 2.2 Recommended: Envoy Gateway

[Envoy Gateway](https://gateway.envoyproxy.io/) is the CNCF reference
implementation of the Kubernetes Gateway API, built on Envoy Proxy.
Reached v1.2 (stable) -- production-ready for all use cases described here.
Cost: **$0** (open source, Apache 2.0).

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

**Cost: $0** (free tier includes community blocklist)

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

This is free (just resource allocation) and prevents cross-service impact.

---

## Part 3: AWS Fronting Architecture (Cost-Conscious)

**The cardinal rule: cost scales with traffic volume.** At PB/s ingestion
scale, per-GB and per-request charges compound horrifically. Every layer
that touches traffic must be evaluated on $/GB.

### 3.1 Estimated Cost Table

Here's what things actually cost at scale, not the marketing pitch.
Prices are US East (Virginia) as of March 2026 -- verify before deploying.

**Estimated Monthly cost at 10 TB/month ingestion (a modest production workload):**

| Service | Monthly Cost | $/GB | Notes |
|---|---|---|---|
| NLB (same-AZ) | ~$76 | ~$0.006 | $16.43 base + ~$60 NLCU |
| NLB (cross-AZ) | ~$276 | ~$0.026 | + $0.01/GB each way cross-zone |
| ALB | ~$96 | ~$0.008 | $16.43 base + ~$80 LCU |
| AWS WAF (10 rules) | ~$21 | ~$0.006/M req | $5 ACL + $10 rules + $6 requests |
| AWS WAF + Bot Control | ~$41+ | varies | + $10 ACL + request charges |
| Shield Standard | $0 | free | L3/L4 only, no L7 |
| Shield Advanced | $3,000 | flat | Includes WAF. 12-month commitment. |
| CloudFront (PAYG) | ~$850 | ~$0.085 | First 10 TB tier. Drops with volume. |
| CloudFront (flat-rate Pro) | $15/mo | ~$0.0003 | Up to 50 TB included, degraded perf after |
| Cloudflare Free | $0 | $0 | Unlimited DDoS + 5 WAF rules |
| Cloudflare Pro | $20/mo | $0 | Managed WAF + 20 rules |
| Cloudflare Spectrum (TCP) | $1/GB | $1.00 | After 5-10 GB free tier. Prohibitive. |
| CrowdSec | $0 | $0 | Community blocklist, K8s native |

**Monthly cost at 100 TB/month (serious production):**

| Service | Monthly Cost | Notes |
|---|---|---|
| NLB (same-AZ) | ~$620 | Dominated by NLCU data processing |
| ALB | ~$820 | Higher LCU rate |
| AWS WAF (10 rules, 100M req) | ~$75 | Scales with request count |
| CloudFront (PAYG) | ~$6,500 | Brutal at volume. |
| Cloudflare Pro | $20/mo | Still $20. Unlimited HTTP traffic. |

### 3.2 Why CloudFront is Wrong for Ingestion

CloudFront is a CDN. It's designed to **serve** content, not **receive** it.

**Problems for data ingestion:**
- PAYG pricing is $0.085/GB at first tier -- brutal at ingestion volume
- Designed to cache and serve, not proxy POST requests to origin
- Adds latency (edge -> origin hop) for non-cacheable traffic
- The flat-rate plans (Pro $15/mo for 50 TB) degrade performance
  (fewer edge locations) when you exceed the allowance
- You're paying for CDN features (caching, edge compute) you don't use

**The one exception:** If you need AWS WAF (which only attaches to
CloudFront, ALB, or API Gateway), then CloudFront becomes a required
intermediary. But question whether you need AWS WAF at all (see 3.4).

### 3.3 Recommended AWS Architecture (Cost-Optimised)

```
Internet
    |
    +-- [Cloudflare DNS + Proxy]  <- $0-20/mo, unlimited DDoS
    |     L3/L4/L7 DDoS mitigation
    |     WAF rules (free tier: 5, Pro: 20)
    |     IP reputation, bot mitigation
    |
    +-- [AWS NLB]  <- ~$0.006/GB, same-AZ preferred
          L4 load balancing
          Static IP (for Cloudflare origin)
          Health checks
          |
          +-- [K8s Service]
                |
                +-- [Envoy Gateway]  <- $0
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

**Total monthly cost (10 TB/month):**

| Component | Cost |
|---|---|
| Cloudflare Pro | $20 |
| AWS NLB (same-AZ) | ~$76 |
| Envoy Gateway | $0 |
| CrowdSec | $0 |
| **Total** | **~$96/mo** |

**Compare to the "just use AWS" approach:**

| Component | Cost |
|---|---|
| ALB | ~$96 |
| AWS WAF (10 rules) | ~$21 |
| Shield Standard | $0 |
| **Total** | **~$117/mo** |
| **...but without:** | L7 DDoS, IP reputation, bot mitigation |

The Cloudflare + NLB approach is cheaper AND provides better protection.

### 3.4 When You DON'T Need AWS WAF

AWS WAF is the right choice when:
- Compliance requires AWS-native security controls
- You need deep integration with AWS services (API Gateway, AppSync)
- You're already on Shield Advanced ($3K/mo) which includes WAF free

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

```
Internet -> NLB -> K8s Service -> dfe-receiver
```
- Cost: ~$16/mo (NLB base)
- Protection: Application-level only (auth, body limits, timeouts)
- Suitable for: Dev, staging, internal networks

#### Variant B: Production Standard

```
Internet -> Cloudflare -> NLB -> Envoy Gateway -> dfe-receiver
```
- Cost: ~$96/mo at 10 TB
- Protection: DDoS, WAF, rate limiting, IP reputation, auth
- Suitable for: Most production deployments

#### Variant C: High-Security / Compliance

```
Internet -> CloudFront + AWS WAF -> ALB -> Envoy Gateway -> dfe-receiver
```
- Cost: ~$950+/mo at 10 TB (CloudFront dominates)
- Protection: Full AWS-native stack, compliance-ready
- Suitable for: Regulated industries, AWS-mandated security controls

#### Variant D: Maximum Protection

```
Internet -> Cloudflare Enterprise -> NLB -> Envoy Gateway + CrowdSec -> dfe-receiver
```
- Cost: Custom (Cloudflare Enterprise pricing)
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

**Cloudflare Spectrum** proxies arbitrary TCP/UDP but costs **$1/GB** after a
tiny free tier (5-10 GB). At ingestion scale, this is prohibitively expensive
and not recommended.

### 3.7 NLB Configuration Notes

**Same-AZ targeting:** To avoid the $0.01/GB cross-zone surcharge, configure
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
    # Disable cross-zone to avoid $0.01/GB surcharge
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

```
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

See Parts 2 and 3 above for configuration details and cost analysis.

---

## Fact-Check Log

Items verified during document creation (March 2026):

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
| NLB base cost ~$16.43/mo (US East) | Yes | [AWS ELB pricing](https://aws.amazon.com/elasticloadbalancing/pricing/) |
| NLB cross-zone surcharge $0.01/GB | Yes | [AWS blog](https://aws.amazon.com/blogs/networking-and-content-delivery/exploring-data-transfer-costs-for-aws-network-load-balancers/) |
| AWS WAF $5/ACL + $1/rule + $0.60/M req | Yes | [AWS WAF pricing](https://aws.amazon.com/waf/pricing/) |
| Shield Advanced $3,000/mo | Yes | [AWS WAF pricing](https://aws.amazon.com/waf/pricing/) |
| Cloudflare Free: unlimited DDoS + 5 WAF rules | Yes | [Cloudflare plans](https://www.cloudflare.com/plans/) |
| Cloudflare Pro: $20/mo + managed WAF | Yes | [Cloudflare Pro](https://www.cloudflare.com/plans/pro/) |
| Cloudflare Spectrum: $1/GB after free tier | Yes | [Cloudflare billing](https://support.cloudflare.com/hc/en-us/articles/360041721872-Billing-for-Spectrum) |
| CloudFront first 10 TB: $0.085/GB (US) | Yes | [CloudFront pricing](https://aws.amazon.com/cloudfront/pricing/) |
| axum slowloris vulnerability | Yes | [GH issue #2741](https://github.com/tokio-rs/axum/issues/2741) |
| Envoy `request_headers_timeout` for slowloris | Yes | [Envoy docs](https://www.envoyproxy.io/docs/envoy/latest/faq/configuration/timeouts) |
| HyperI standard: Gateway API over Ingress | Yes | Project K8s standards |

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
