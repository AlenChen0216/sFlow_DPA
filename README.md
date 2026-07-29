# UDP/8888 DPA receive path with direct x86 result access

This project loads a DOCA DPA program, steers IPv4/UDP destination port 8888
into a DPA-owned Ethernet receive queue, lets the DPA process each completion,
and publishes a result record into memory owned by the host process.

## How the x86 host reads `sflow_dedicated_output`

Run `sflow_dpa_host` on the x86 host, not on the BlueField Arm cores. The x86
process allocates `struct sflow_host_memory`, whose first member is
`struct sflow_dedicated_output`, and registers the allocation as:

- an ibverbs memory region for NIC receive DMA; and
- a DOCA mmap exported to the DPA for packet access and result publication.

Only the control process and result allocation move to x86; `dev/kernel.c`
still executes on the BlueField DPA.

The host passes the mmap's DPA handle and the x86 virtual address to
`sflow_receive_rpc()`. The DPA obtains an external-memory pointer with
`doca_dpa_dev_mmap_get_external_ptr()`, writes the result, and calls
`__dpa_thread_window_writeback()`. After the blocking `doca_dpa_rpc()` returns,
the x86 code validates and copies `resources.host_memory->output` into a local
snapshot before printing it.

This follows NVIDIA's documented host-memory path and avoids a separate
Arm-to-x86 message service. DPA heap is a different address space; if the
result is moved to DPA heap, use `doca_dpa_d2h_memcpy()` instead.

## Data path

```text
wire UDP/8888
      |
      v
DOCA Flow exact match
      |
      v
DOCA Verbs ETH RQ ---- DMA ----> registered host packet slots
      |                              |
      v                              v
DPA completion -> modify_packet() -> store_dedicated_data()
                                      |
                                      v
                           registered host output record
                                      |
                                      v
                              host prints/consumes it
```

The binary receives a finite batch so the whole registration path can be
tested without adding application policy. The default is one packet; up to 64
can be requested.

## Build and run on the x86 host

Requirements:

- BlueField-3 or a supported newer DPA device
- matching DOCA-Host 3.3 runtime and development packages on x86
- FlexIO/DPACC toolchain
- root privileges and a PF Ethernet `mlx5` device

Do not copy the existing `build/sflow_dpa_host` from BlueField to the PC. A
binary built on BlueField is AArch64. Copy or clone the source tree and build
it natively on x86:

```bash
cd sFlow_DPA
./scripts/check_x86_host.sh
meson setup build-x86
meson compile -C build-x86
file build-x86/sflow_dpa_host
```

The final `file` command must report an x86-64 executable. The build compiles
`dev/kernel.c` with DPACC and links its generated x86 host archive into
`sflow_dpa_host`.

### One-time DPA EU setup

NVIDIA requires DPA EU resources to be configured before a DPA application is
run from the external host. On the BlueField Arm OS, first inspect the ECPF,
the host PF vHCA ID, and existing partitions:

```bash
sudo mst start
sudo mst status -v
sudo /opt/mellanox/doca/tools/dpaeumgmt partition info -d <ECPF-mlx5-device>
sudo /opt/mellanox/doca/tools/dpaeumgmt partition query -d <ECPF-mlx5-device>
```

If the host PF is not already assigned a partition, the BlueField
administrator must create one using free EUs and the host PF's actual vHCA ID:

```bash
sudo /opt/mellanox/doca/tools/dpaeumgmt partition create \
  -d <ECPF-mlx5-device> \
  -v <host-PF-vHCA-ID> \
  -r <free-EU-range> \
  -m <reserved-EU-group-count>
```

Do not guess these values or create a second overlapping partition. EU
partitioning is system-wide administration, and `dpaeumgmt` refuses changes
while a DPA process is active.

### Receive and consume the result

On x86, identify the PF and start the program:

```bash
ibv_devices
sudo ./build-x86/sflow_dpa_host mlx5_0
```

To wait for a batch of 16 matching packets:

```bash
sudo ./build-x86/sflow_dpa_host mlx5_0 16
```

Send IPv4 UDP traffic to destination port 8888 on the selected physical port.
The program returns after the requested batch and prints the host-owned
snapshot. A successful x86 run begins with:

```text
Result owner: x86_64 host process (direct DPA-to-host memory)
```

The consumption point for integration into another x86 application is
`snapshot_output()` in `host/sflow_dpa_host.c`. Do not run the Arm and x86
control programs against the same PF at the same time.

If the architecture must instead remain as two processes—DPA controlled by a
BlueField Arm application and results consumed by a separate x86
application—this direct pointer design does not apply. Keep the Arm allocation
and add DOCA Comch or RDMA as a transport.

## Where to add your logic

See [TODO.md](TODO.md). The only intentionally incomplete data-path functions
are:

- `modify_packet()` in `dev/kernel.c`
- `store_dedicated_data()` in `dev/kernel.c`

## NVIDIA references

- [DOCA DPA](https://docs.nvidia.com/doca/sdk/doca-dpa.pdf)
- [DPA Development and memory coherency](https://docs.nvidia.com/doca/sdk/dpa-development.pdf)
- [DOCA-Host installation](https://docs.nvidia.com/doca/sdk/doca-host-installation-and-upgrade.pdf)
- [DPA EU management](https://docs.nvidia.com/doca/sdk/nvidia-doca-dpa-execution-unit-management-tool.pdf)
- [DOCA Verbs](https://docs.nvidia.com/doca/sdk/doca-verbs/)
- [DOCA Flow](https://docs.nvidia.com/doca/sdk/doca-flow/)
