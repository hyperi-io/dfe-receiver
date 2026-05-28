# Real-world flow corpus

Real captures and raw UDP-payload fixtures from reference flow collectors,
used by the flow handler integration tests to validate decoder correctness
against real-world emitter output (not just our hand-crafted unit-test
packets). Hand-crafted unit tests overfit to the spec; real captures catch
quirks like count=0 keep-alives, padded packets, jumbo templates, and
vendor-specific PEN information elements.

## Two file types

- **`*.bin`** -- raw UDP-payload bytes, exactly what arrives on the wire
  after the kernel strips Ethernet/IP/UDP. Feed directly to the decoder.
- **`*.pcap`** -- libpcap file containing UDP datagrams. Tests walk the
  PCAP, strip the common Ethernet+IPv4+UDP prefix (42 bytes), and feed the
  resulting bytes to the decoder.

`.bin` is the simpler form -- preferred for new fixtures because there's no
L2/L3/L4 stripping heuristic to maintain.

## Files

| File | Source | License | Notes |
|---|---|---|---|
| `telegraf-netflow-v5.bin` | influxdata/telegraf `plugins/inputs/netflow/testcases/netflow_v5_example/netflow_v5.bin` | MIT | NetFlow v5 datagram, real softflowd emit |
| `telegraf-netflow-v9.bin` | influxdata/telegraf `plugins/inputs/netflow/testcases/netflow_v9_example/netflow_v9.bin` | MIT | NetFlow v9 datagram |
| `telegraf-netflow-v9-options.bin` | influxdata/telegraf `plugins/inputs/netflow/testcases/netflow_v9_options/message.bin` | MIT | NetFlow v9 with options templates |
| `telegraf-ipfix-example.bin` | influxdata/telegraf `plugins/inputs/netflow/testcases/ipfix_example/ipfix_0.bin` | MIT | IPFIX datagram (first in sequence) |
| `telegraf-ipfix-pen-35632.bin` | influxdata/telegraf `plugins/inputs/netflow/testcases/ipfix_pen_35632/message-1.bin` | MIT | IPFIX with private enterprise number 35632 (NetScaler) |
| `telegraf-sflow-v5.bin` | influxdata/telegraf `plugins/inputs/netflow/testcases/sflow_v5_example/sflow_v5.bin` | MIT | sFlow v5 datagram, flow samples |
| `telegraf-sflow-issue-15918.bin` | influxdata/telegraf `plugins/inputs/netflow/testcases/sflow_issue_15918/message.bin` | MIT | sFlow v5 regression fixture from telegraf issue #15918 |
| `telegraf-sflow-issue-18876.bin` | influxdata/telegraf `plugins/inputs/netflow/testcases/sflow_issue_18876/message.bin` | MIT | sFlow v5 regression fixture from telegraf issue #18876 |
| `netgauze-pmacct-ipfix-cisco-100.pcap` | NetGauze `assets/pcaps/pmacct-tests/100-IPFIXv10-CISCO/traffic-00.pcap` | Apache-2.0 | Real Cisco IPFIX capture, used by pmacct integration tests, mirrored in NetGauze pcap-decoder test data |
| `netgauze-pmacct-nfv9-cisco-101.pcap` | NetGauze `assets/pcaps/pmacct-tests/101-NFv9-CISCO-cust_primitives/traffic-00.pcap` | Apache-2.0 | Real Cisco NetFlow v9 capture with custom primitives |

## Source repos

- influxdata/telegraf -- <https://github.com/influxdata/telegraf> -- MIT
- NetGauze/NetGauze -- <https://github.com/NetGauze/NetGauze> -- Apache-2.0

Both licenses are compatible with this project's BUSL-1.1 -- they
permit redistribution of the fixture bytes without any restriction beyond
attribution (handled by this README).

## Why these and not Wireshark wiki samples

The Wireshark SampleCaptures wiki has no NetFlow/IPFIX/sFlow PCAPs in the
main captures section -- and the Wireshark project itself is GPL-2.0 which
would be license-incompatible. NetGauze and Telegraf both ship real-world
captures explicitly intended for parser testing under permissive licenses.

## Expected decode behaviour

| File | Records expected | Notes |
|---|---|---|
| `netgauze-pmacct-ipfix-cisco-100.pcap` | 12 across 6 datagrams | Cisco IPFIX, mixed templates + data |
| `netgauze-pmacct-nfv9-cisco-101.pcap` | 51 across 40 datagrams | Cisco NetFlow v9, mixed templates + data |
| `telegraf-ipfix-example.bin` | 0 | Template-only IPFIX packet (legitimate) |
| `telegraf-ipfix-pen-35632.bin` | 0 | Template-only IPFIX with PEN 35632 (legitimate) |
| `telegraf-netflow-v5.bin` | 8 | NetFlow v5 has no templates -- all data |
| `telegraf-netflow-v9.bin` | 8 | NetFlow v9 self-contained: templates + data |
| `telegraf-netflow-v9-options.bin` | 8 | NetFlow v9 with options templates + data |
| `telegraf-sflow-v5.bin` | 1 | sFlow v5 with one flow sample |
| `telegraf-sflow-issue-15918.bin` | 7 | sFlow v5 regression fixture |
| `telegraf-sflow-issue-18876.bin` | 0 | sFlow v5 counter-samples-only (legitimate) |

**Template-only packets are not bugs.** IPFIX and NetFlow v9 separate
template definitions from data records. The first packet from a new
exporter is often template-only. Our decoder ingests templates and
returns 0 data records -- exactly the wire behaviour.

## Verifying integrity

```bash
cd tests/fixtures/flow/external
sha256sum -c sha256sums.txt
```

The `corpus_sha256_matches_manifest` integration test also runs this check
on every test run -- silent fixture rot will fail loudly.

## Adding a new fixture

1. Verify the source is CC0 / BSD / Apache-2.0 / MIT / public domain
   (compatible with BUSL-1.1). Reject GPL, AGPL, SSPL, BSL, LGPL,
   unlicensed.
2. Download with `curl -L -o <name>.<ext> <URL>` (use `.bin` for raw UDP
   payloads, `.pcap` for libpcap captures)
3. Compute SHA256 and add a line to `sha256sums.txt`
4. Add a row to the table above with source URL + license + notes
5. Add file-specific expectations to the integration test if you need to
   assert protocol-specific counts
