# NXLog

NXLog Community Edition (CE) and Enterprise Edition (EE) ship to dfe-receiver with no receiver changes. Keep the output module you run today and point it at the matching listener.

| NXLog output | dfe-receiver listener | Edition |
|---|---|---|
| `om_http` with `xm_json` | HTTP `POST /ingest`, port 8080 | CE and EE |
| `om_tcp` or `om_ssl` with `xm_syslog`, `OutputType Syslog_TLS` | syslog TCP 514, TLS 6514 | CE and EE |
| `om_tcp` with `xm_gelf`, `OutputType GELF_TCP` | GELF TCP 12201 | CE and EE |
| `om_kafka` | straight to a source's landing topic, as an [external origin](https://github.com/hyperi-io/dfe-engine/blob/main/docs/data-plane/external-pipelines.md) | EE only |

Start with HTTP. Each record arrives as JSON with every field NXLog holds, so nothing is lost to a syslog or GELF mapping, and the receiver can route on NXLog's own fields. Every input sets `$SourceModuleName`, the name of its `<Input>` block, and `$SourceModuleType`, its module (`im_file`, `im_msvistalog` and so on). A routing rule keys on one of them with nothing added at the agent:

```yaml
field:    SourceModuleName
operator: equals
value:    windows_security
```

## HTTP

```text
<Extension json>
    Module  xm_json
</Extension>

<Input windows_security>
    Module  im_msvistalog
</Input>

<Input app_log>
    Module  im_file
    File    '/var/log/app/*.log'
    # im_file puts the line in $raw_event, and to_json() leaves $raw_event out.
    Exec    $Message = $raw_event;
</Input>

<Output dfe>
    Module           om_http
    URL              https://dfe-receiver.example.com:8080/ingest
    ContentType      application/json
    HTTPSCAFile      /etc/nxlog/dfe-ca.pem
    HTTPSCertFile    /etc/nxlog/client.pem
    HTTPSCertKeyFile /etc/nxlog/client.key
    Exec             to_json();
</Output>

<Route to_dfe>
    Path    windows_security, app_log => dfe
</Route>
```

`im_msvistalog` runs on Windows only, and the CE Linux image does not carry it. On Linux, delete the `windows_security` input and its name in the route. Left in, CE logs a module load error and routes the other inputs.

CE `om_http` has no directive for an extra header, so it can't send a bearer token. Authenticate it with a client certificate: set the HTTP listener's `auth.mode` to `mtls`, with `server.tls.enabled: true`, `server.tls.client_auth: required` and a `server.tls.ca_file`, and give NXLog `HTTPSCertFile` and `HTTPSCertKeyFile`. The receiver refuses `mtls` when TLS is off. EE adds `AddHeader`, so a bearer token works there as well.

`to_json()` drops `$raw_event` and any field starting with `.` or `_`. An input that puts its text in `$raw_event` alone, as `im_file` and `im_tcp` do, needs `$Message = $raw_event;` first. Without it the record arrives with no log line.

CE sends one event per POST and waits for a 200, 201 or 202 before the next. It reconnects and resends after a timeout or a dropped connection, so a record is never lost in transfer, but the throughput is low. Any other status makes it resend the same event, including the 503 a receiver returns while its sink cannot confirm. EE's `BatchMode` puts many events in one POST, and the receiver splits a JSON array or newline-delimited body into one event each.

## Syslog and GELF

Use these where HTTP can't go, or where the agent speaks one of them today.

```text
<Extension syslog>
    Module  xm_syslog
</Extension>

<Output dfe>
    Module      om_ssl
    Host        dfe-receiver.example.com
    Port        6514
    CAFile      /etc/nxlog/dfe-ca.pem
    OutputType  Syslog_TLS
    Exec        to_syslog_ietf();
</Output>
```

The output sends nothing until a `<Route>` sends an input to `dfe`, as in the HTTP example.

`Syslog_TLS` is NXLog's name for RFC 5425 octet counting, and it works over plain `om_tcp` as well as `om_ssl`. The receiver reads octet-counted and newline-framed syslog on the same port. Syslog carries the message text and a handful of header fields, so most of what NXLog parsed doesn't survive it.

```text
<Extension gelf>
    Module  xm_gelf
</Extension>

<Output dfe>
    Module      om_tcp
    Host        dfe-receiver.example.com
    Port        12201
    OutputType  GELF_TCP
    Exec        $ShortMessage = $Message;
</Output>
```

The GELF listener takes TCP and splits on a NUL byte, which is `xm_gelf`'s default (`UseNullDelimiter TRUE`). It has no UDP port, so `GELF_UDP` has nowhere to land. `xm_gelf` cuts `short_message` to 64 characters unless the event carries a `$ShortMessage` field, hence the `Exec` line. The `Exec` copies `$Message`, and an `im_file` input leaves that unset. Add the `$Message = $raw_event` line from the HTTP section to the input first. Without it, `short_message` keeps the first 64 characters of the line and drops the rest.

The GELF listener is off by default. Set `gelf.enabled: true` in the receiver config. An enabled listener refuses to start unless `gelf.accept_unauthenticated: true` is set, or `gelf.tls.enabled: true` is set with `gelf.tls.client_auth: required` and a `gelf.tls.ca_file`. `accept_unauthenticated` takes every sender that reaches the port, so pair it with `server.ip_filter.cidrs`.
