# Setting up an inbound webhook caller

`POST /webhook/{caller}` accepts events pushed by a product -- an alert rule, a
SaaS notification hook -- rather than streamed by an agent. This page is how to
configure one end to end without reading the Rust. The design rationale and the
request flow are in [DESIGN.md](DESIGN.md); the field-by-field grammar is in
`config.example.yaml` and the generated `docs/config-schema.yaml`.

Every caller is declared separately and carries its own secret, topic and body
shape, so two callers never share a credential and a wrong secret is only ever
tried against the caller the URL path names.

## The six decisions

| Decision | Key | What hangs on it |
|---|---|---|
| Which listener | `webhook.bind_address` | Unset shares the ingest port; set gives the intake its own port so only that port is reachable from the product's egress |
| How the sender proves itself | `callers[].auth.mode` | `hmac` if the product can sign, `header` if it can only attach a fixed header |
| Where the secret comes from | `callers[].auth.secret_source` | A `provider:path[:key]` reference, never a literal in the file |
| How stale is too stale | `callers[].auth.tolerance_secs` | The replay window, `hmac` only |
| How big a body is allowed | `webhook.max_body_size` | Applies to the intake alone, independently of `server.max_body_size` |
| What reaches the topic | `callers[].body`, `callers[].filter`, `callers[].topic` | One object or an array to fan out, an optional CEL filter, and the topic name used verbatim |

## Choosing the authentication mode

`hmac` is the mode to use whenever the product supports it. The sender computes
HMAC-SHA256 over the string `{timestamp}.{body}` and sends the hex digest in a
signature header, with the same unix-seconds timestamp in its own header. That
proves both that the secret holder sent the request and that the body has not
been altered, and the timestamp bounds replay.

`header` exists because some products can only attach a static header to a
webhook. It proves the sender holds the secret and nothing more: no body
integrity, and no replay protection. Restrict the source addresses with
`server.ip_filter` when using it.

Authentication runs before the readiness check, so an unauthenticated client
learns nothing about the pipeline's state. Failures log at debug and count under
`dfe_receiver_auth_failures_total{reason}`, so a credential spray does not write
a warn line per attempt.

## Secret references

`secret_source` is a `provider:path[:key]` reference resolved through the same
reader as bearer tokens and refreshed on `refresh_interval_secs`. A caller whose
secret cannot be read refuses to start rather than serving unauthenticated --
a missing secret is a configuration error, not a runtime degradation.

## Worked example: an asset context source

The value of this shape is not the feed on its own. It is that asset and
identity records join to observability facts, so a noisy signal gains the
context needed to judge it -- which host, owned by whom, running what, exposed
where. Alert rules from an asset-inventory product are the shipped precedent.

```yaml
webhook:
  enabled: true
  bind_address: "0.0.0.0:8090"     # own port: expose only this to the product
  max_body_size: 1048576
  callers:
    - name: asset_context
      topic: asset_context_land
      auth:
        mode: header               # the product can only attach a static header
        secret_source: "vault:kv/data/dfe/webhooks:asset_context"
        refresh_interval_secs: 300
        header: x-webhook-secret
      body: single
```

Records land on `asset_context_land` stamped with `_source: asset_context` and
`_timestamp_receiver`. The `_source` stamp is the caller name and wins over any
value the sender supplies, so the join key cannot be spoofed by the payload.

Pair it with `server.ip_filter` restricted to the product's published egress
range, because `header` mode carries no body integrity.

## Worked example: an observability feed

An alerting platform that can attach an `Authorization` header fits `header`
mode directly -- the header accepts a bare secret or a `Bearer <secret>` form.
A platform that signs its payloads should use `hmac` instead.

```yaml
webhook:
  enabled: true
  callers:                          # bind_address unset: shares the ingest port
    - name: alerting
      topic: alerting_land
      auth:
        mode: hmac
        secret_source: "vault:kv/data/dfe/webhooks:alerting"
        refresh_interval_secs: 300
        header: x-signature
        timestamp_header: x-timestamp
        tolerance_secs: 300
      body: array                   # the platform batches alerts per POST
      filter: 'severity == "critical" || severity == "high"'
```

`body: array` checks, filters and stamps every element before delivering any of
them, so one element that is not an object refuses the whole request with
nothing on the topic. A sender that retries therefore duplicates nothing.

The CEL filter is compiled at load, so a malformed expression is a startup
error rather than a per-request surprise. A record the filter drops still
answers 202: the sender did nothing wrong and must not retry.

On the shared listener the caller's secret is the only credential needed --
`server.auth` does not apply to the webhook routes -- while `server.tls`,
`server.ip_filter` and `server.rate_limit` all still do.

## What the sender will see

| Status | Meaning | `error` field |
|---|---|---|
| 202 | Accepted, including a record the filter dropped | -- |
| 400 | The body shape is wrong for the declared `body` | `body_not_an_array`, `record_not_an_object` |
| 401 | Authentication failed | `missing_signature`, `invalid_signature`, `stale_signature`, `missing_auth_header`, `invalid_header_value` |
| 404 | No caller of that name is declared | `unknown_caller` |
| 413 | Over `webhook.max_body_size`; counted, never sent to the DLQ | -- |
| 429 | `server.rate_limit` exhausted; carries `retry-after` | -- |
| 503 | The pipeline is under memory pressure; carries `retry-after` | -- |

A 401 is the sender's problem and a 503 is ours, so treat them differently when
configuring the product's retry behaviour: 503 and 429 should be retried, and a
401 should page whoever rotated the secret.

## Proving it works

Send a signed request by hand. The signature is HMAC-SHA256 over
`{timestamp}.{body}`, so the timestamp must be the one in the header:

```bash
TS=$(date +%s)
BODY='{"severity":"high","host":"example"}'
SIG=$(printf '%s.%s' "$TS" "$BODY" \
  | openssl dgst -sha256 -hmac "$WEBHOOK_SECRET" -hex \
  | sed 's/^.*= //')
curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST "https://receiver.example/webhook/alerting" \
  -H 'content-type: application/json' \
  -H "x-timestamp: $TS" \
  -H "x-signature: sha256=$SIG" \
  --data "$BODY"
```

Then confirm the record arrived rather than assuming the 202 means delivery:
read the caller's topic, and check `dfe_receiver_requests_success` moved and
`dfe_receiver_auth_failures_total` did not. The integration suite covers both
halves -- the intake's behaviour in `tests/integration/webhook.rs`, and the path
through to a real broker in `tests/integration/protocol_kafka_roundtrip.rs`.

## Reaching the receiver from a cloud sender

A sender outside the network needs a resolvable HTTPS endpoint with a publicly
trusted certificate, which an internal certificate authority cannot provide.
Prefer an outbound tunnel over opening an inbound port: the tunnel dials out, so
no public address or inbound rule is needed on the receiver's network.

Some cloud notification services cannot attach custom headers at all and sign
with their own certificate over an envelope that wraps the payload as a string.
Neither shipped auth mode authenticates that shape, so put a small function
between the service and the intake to unwrap the payload and attach the caller's
header. Do not weaken the intake to accommodate a sender.
