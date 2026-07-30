# Application TODOs

The receive queues, memory registration, DPA application loading, UDP/8888
steering, completion processing, DPU-side publication, Comch server, framed
transfer, and x86 client reassembly are already wired. Your
application-specific work is intentionally limited to these two functions in
`dev/kernel.c`:

1. `modify_packet()`

   - Parse the Ethernet/IP/UDP headers safely.
   - Validate every offset against `packet_length`.
   - Modify the required header or payload bytes.
   - If you change IP/UDP contents, update lengths and checksums.
   - This starter is receive-only. Add a DPA Ethernet send queue if the changed
     packet must go back onto the wire.

2. `store_dedicated_data()`

   - Extract the keys or fields needed by the host.
   - Store no more than `SFLOW_DEDICATED_DATA_CAPACITY` entries.
   - Keep `output->dedicated_data_count` equal to the number of occupied entries.
   - Keep multibyte field byte order documented in
     `common/sflow_dpa_common.h`.

Before production use:

- Replace the finite RPC batch with a DPA thread/event-driven loop if the
  receiver must run indefinitely. In that design, progress Comch concurrently
  and define when a requested snapshot becomes consistent.
- Decide whether one latest-value record is sufficient. For multiple records,
  define a ring with producer/consumer indices and explicit overrun behavior.
- Add malformed/truncated/VLAN/IPv6 packet tests for the parser you implement.
- Add a transmit queue only if packets must be forwarded or reflected.
- Measure external-memory access cost. For a hot path, stage work in DPA
  memory and publish compact records to BlueField Arm memory in batches.
