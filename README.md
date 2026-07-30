# sFlow DPA receiver with a DOCA Comch output service

This project has two communication variants, each with a DPU server and an x86
client:

- `sflow_dpa_comch_server` runs on the BlueField Arm cores. It loads the DPA
  program, steers IPv4/UDP destination port 8888 into a DPA receive queue,
  captures a finite packet batch, and exposes the resulting
  `struct sflow_dedicated_output` through a DOCA Comch server.
- `sflow_dpa_comch_client` runs on the x86 host. It connects to the server,
  sends a `GET_OUTPUT` request, reassembles the response, validates it, and
  prints the result.
- `sflow_dpa_comch_server_producer_consumer` and
  `sflow_dpa_comch_client_producer_consumer` preserve the same capture and
  request behavior but transfer the large snapshot through the high-speed
  Comch producer/consumer data path.

The server remains available after a response, so clients can request the
captured snapshot sequentially. Press Ctrl-C on the DPU to stop it.

## Data and control paths

```text
wire UDP/8888
      |
      v
DOCA Flow exact match
      |
      v
DPA Ethernet RQ -- DMA --> BlueField Arm registered packet slots
      |                              |
      v                              v
DPA completion -> packet logic -> sflow_dedicated_output in Arm memory
                                      |
x86 GET_OUTPUT ---- DOCA Comch ------>|
x86 output       <--- framed chunks --+
```

`sflow_dedicated_output` contains 1,048,576 hash-table entries and is currently
138,412,096 bytes, which is larger than one Comch control message. The shared
protocol in `common/sflow_comch_protocol.h` therefore frames the snapshot into
sequential chunks. The server queries the device's maximum Comch message size
instead of assuming a fixed limit. Each frame contains the request ID, output
ABI version, total length, byte offset, and payload length. The client rejects
missing, incompatible, or out-of-order frames.

### Producer/consumer fast path

The copied producer/consumer pair uses the control channel only for the
`GET_OUTPUT` request and error responses:

```text
x86 consumer: register output memory and post receive buffer
        |
        +---- GET_OUTPUT + consumer buffer limit ----> DPU control channel
                                                       |
DPU producer: map snapshot, send doca_buf chunks -------+
        |
        +---- DMA/PCIe fast path + immediate header ---> x86 output memory
```

The client advertises its maximum consumer buffer size in the request. The
server chooses the smaller of that value and its producer capability for every
chunk. Each producer task directly references the snapshot; the matching
consumer task receives directly into the final output structure. The 32-byte
header is immediate data, so payload bytes are not copied into control-channel
frames. The separate protocol is defined in
`common/sflow_comch_producer_consumer_protocol.h`.

The client posts one receive buffer at a time. If the producer reaches the next
chunk before the replacement buffer is advertised, the server treats
`DOCA_ERROR_AGAIN` as backpressure and retries that offset from its progress
loop.

After copying the finite DPA result into the output snapshot, the
producer/consumer server releases its DPA, Verbs, Flow, and external-PD device
resources before opening the standard Comch device. Comch fast-path consumer
mmap import does not support a device backed by an external protection domain.

## Requirements

- BlueField-3 or a supported newer DPA device
- matching DOCA 3.3 development/runtime packages on the DPU and x86 host
- FlexIO/DPACC toolchain on the DPU
- root privileges
- a PF Ethernet `mlx5` device

The Meson build is architecture-aware: it builds only the server targets on
`aarch64` and only the client targets on `x86_64`.

## Build the DPU server

Run on the BlueField Arm OS:

```bash
cd sFlow_DPA
chmod +x scripts/check_dpu.sh
./scripts/check_dpu.sh
meson setup build-dpu
meson compile -C build-dpu
file build-dpu/sflow_dpa_comch_server
```

The final command must report an AArch64 executable. DPACC compiles
`dev/kernel.c` and links the generated host archive into the server.
The build also produces
`build-dpu/sflow_dpa_comch_server_producer_consumer`.

## Build the x86 client

Copy or clone the same source tree on the x86 host, then run:

```bash
cd sFlow_DPA
./scripts/check_x86_host.sh
meson setup build-x86
meson compile -C build-x86
file build-x86/sflow_dpa_comch_client
```

The final command must report an x86-64 executable. The x86 build needs
`doca-common` and `doca-comch`; it does not compile the DPA device program.
The build also produces
`build-x86/sflow_dpa_comch_client_producer_consumer`.

## Run

First identify:

- the DPU `mlx5` PF used for the DPA receive path;
- on the DPU, the net representor PCI address corresponding to the host PF;
- on x86, the BlueField PF PCI address used by the Comch client.

Start the DPU server. The last argument is optional and defaults to one packet:

```bash
sudo ./build-dpu/sflow_dpa_comch_server \
  <dpu-mlx5-device> <host-PF-representor-pci> [packet-count]
```

For example, to capture 16 packets:

```bash
sudo ./build-dpu/sflow_dpa_comch_server mlx5_0 0000:03:00.0 16
```

Send the requested number of IPv4 UDP packets to destination port 8888. Once
the DPA batch completes, the server prints:

```text
Comch server 'sflow-dpa-output' is ready; press Ctrl-C to stop
```

Then request the result from x86:

```bash
sudo ./build-x86/sflow_dpa_comch_client <BlueField-PF-pci-address>
```

For example:

```bash
sudo ./build-x86/sflow_dpa_comch_client 0000:03:00.0
```

The client exits after receiving and printing the complete
`sflow_dedicated_output`. If the server is not ready, the client waits for the
named Comch service. Use Ctrl-C to cancel.

To use the producer/consumer pair instead, run:

```bash
# BlueField Arm
sudo ./build-dpu/sflow_dpa_comch_server_producer_consumer \
  <dpu-mlx5-device> <host-PF-representor-pci> [packet-count]

# x86 host, after the DPU prints that the producer server is ready
sudo ./build-x86/sflow_dpa_comch_client_producer_consumer \
  <BlueField-PF-pci-address>
```

The producer/consumer pair uses the service name `sflow-dpa-output-pc`, so it
cannot accidentally connect to the legacy control-channel-only pair.

Do not run another control program against the same receive PF at the same
time. The server captures its finite DPA batch before entering the Comch
progress loop, so a client request should be made only after the ready message.

## Protocol compatibility

The frame header uses network byte order. The output payload is a byte-for-byte
snapshot of `struct sflow_dedicated_output`; BlueField Arm and supported x86
hosts are both little-endian and use the same shared structure definition. The
client checks:

- Comch protocol magic and version;
- request ID and response status;
- `SFLOW_OUTPUT_ABI_VERSION`;
- exact structure length;
- contiguous chunk offsets;
- basic output field bounds.

Change `SFLOW_OUTPUT_ABI_VERSION` whenever the output structure layout or field
meaning changes.

## Application logic

The receive queues, memory registration, DPA application loading, UDP/8888
steering, output publication, Comch request handling, chunking, and x86
reassembly are wired. Application-specific packet transformation and data
extraction remain in:

- `modify_packet()` in `dev/kernel.c`
- `store_dedicated_data()` in `dev/kernel.c`

See `TODO.md` for the remaining production considerations.

## NVIDIA references

- [DOCA Comch](https://docs.nvidia.com/doca/sdk/doca-comch/index.html)
- [DOCA DPA](https://docs.nvidia.com/doca/sdk/doca-dpa/index.html)
- [DOCA Verbs](https://docs.nvidia.com/doca/sdk/doca-verbs/index.html)
- [DOCA Flow](https://docs.nvidia.com/doca/sdk/doca-flow/index.html)
