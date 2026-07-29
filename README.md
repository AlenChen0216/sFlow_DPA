# UDP/8888 DPA receive skeleton

This project loads a DOCA DPA program, steers IPv4/UDP destination port 8888
into a DPA-owned Ethernet receive queue, lets the DPA process each completion,
and publishes a result record into memory owned by the host process.

The host buffer is intentionally host-owned. It is registered as:

- an ibverbs memory region for NIC receive DMA; and
- a DOCA mmap exported to the DPA for packet access and result publication.

That is the cleanest answer to “how can the host read DPA-produced data?”:
the DPA writes a registered host buffer, and the host reads its own virtual
address. DPA heap is a separate address space; when DPA heap is used instead,
the host must explicitly copy it with `doca_dpa_d2h_memcpy()`.

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

## Build

Requirements:

- BlueField-3 or a supported newer DPA device
- DOCA SDK 3.3-compatible headers/libraries
- FlexIO/DPACC toolchain
- root privileges and a PF Ethernet `mlx5` device

```bash
meson setup build
meson compile -C build
```

The build compiles `dev/kernel.c` with DPACC and links its generated host
archive into `sflow_dpa_host`.

## Run

On the machine that owns the PF and should consume the result:

```bash
sudo ./build/sflow_dpa_host mlx5_0
```

To wait for a batch of 16 matching packets:

```bash
sudo ./build/sflow_dpa_host mlx5_0 16
```

Send IPv4 UDP traffic to port 8888 on the selected port. The program returns
after the requested number of matching packets and prints the shared result.
Until you implement the two TODO hooks, `dedicated_data_length` is zero while
the packet count, length, and timestamp prove the DPA-to-host path.

Run this host program on the x86 host if the x86 host must read the result
directly. If it runs on the BlueField Arm cores, the allocation belongs to the
DPU OS; exporting that separate Arm allocation to x86 is a different problem
and requires a host/DPU transport such as DOCA Comch.

## Where to add your logic

See [TODO.md](TODO.md). The only intentionally incomplete data-path functions
are:

- `modify_packet()` in `dev/kernel.c`
- `store_dedicated_data()` in `dev/kernel.c`

## NVIDIA references

- [DOCA DPA](https://docs.nvidia.com/doca/sdk/doca-dpa/)
- [DOCA DPA Verbs](https://docs.nvidia.com/doca/sdk/doca-dpa-verbs/)
- [DOCA RDMA Verbs](https://docs.nvidia.com/doca/sdk/doca-rdma-verbs/)
- [DOCA Flow](https://docs.nvidia.com/doca/sdk/doca-flow/)
- [DPA Development](https://docs.nvidia.com/doca/sdk/dpa-development/)
