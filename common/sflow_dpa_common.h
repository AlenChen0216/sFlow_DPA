/*
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Data shared by the host control plane and the DPA program.
 */

#ifndef SFLOW_DPA_COMMON_H
#define SFLOW_DPA_COMMON_H

#include <stddef.h>
#include <stdint.h>

#define IP_OFFSET 34U
#define UDP_OFFSET 42U
#define SEG_OFFSET 100U
#define SFLOW_UDP_DST_PORT 8888U
#define SFLOW_RX_QUEUE_ID 0U
#define SFLOW_RX_QUEUE_DEPTH 64U
#define SFLOW_MAX_PACKET_SIZE 2048U
#define SFLOW_DEDICATED_DATA_CAPACITY 1048576U
#define SFLOW_OUTPUT_ABI_VERSION 2U


struct stored_data{
	uint32_t cnt;
};

struct hash_entry{
	char key[128];
	struct stored_data data;
};

/*
 * The host owns this output object. The DPA updates it after processing each
 * packet and flushes its external-memory window before returning from the RPC.
 */
struct sflow_dedicated_output {
	uint64_t packets_received;
	uint64_t last_packet_timestamp;
	uint32_t abi_version;
	uint32_t last_packet_length;
	uint32_t dedicated_data_count;
	uint32_t reserved;
	struct hash_entry dedicated_data[SFLOW_DEDICATED_DATA_CAPACITY];
} __attribute__((aligned(64)));

/*
 * One control-process allocation is registered twice:
 *
 *  - as an ibverbs MR, so the NIC receive queue can DMA packets into it;
 *  - as a DOCA mmap, so DPA code can obtain an external-memory pointer.
 *
 * Keeping both areas in one object also makes their lifetime identical.
 */
struct sflow_host_memory {
	struct sflow_dedicated_output output;
	uint8_t rx_slots[SFLOW_RX_QUEUE_DEPTH][SFLOW_MAX_PACKET_SIZE];
} __attribute__((aligned(4096)));

#define SFLOW_RX_SLOTS_OFFSET ((uint64_t)offsetof(struct sflow_host_memory, rx_slots))
#define SFLOW_OUTPUT_OFFSET ((uint64_t)offsetof(struct sflow_host_memory, output))

_Static_assert(SFLOW_DEDICATED_DATA_CAPACITY == 1048576U,
	       "The dedicated-data table must contain 1,048,576 elements");
_Static_assert((SFLOW_DEDICATED_DATA_CAPACITY &
		(SFLOW_DEDICATED_DATA_CAPACITY - 1U)) == 0,
	       "The dedicated-data table capacity must be a power of two");
_Static_assert((sizeof(struct sflow_dedicated_output) % 64U) == 0,
	       "DPA output size must preserve 64-byte window alignment");
_Static_assert(sizeof(struct sflow_dedicated_output) <= UINT32_MAX,
	       "Comch framing uses a 32-bit total length");
_Static_assert((offsetof(struct sflow_host_memory, rx_slots) % 64U) == 0,
	       "Every receive slot must begin on a 64-byte boundary");
_Static_assert((SFLOW_MAX_PACKET_SIZE % 64U) == 0,
	       "Every receive slot must remain 64-byte aligned");

#endif /* SFLOW_DPA_COMMON_H */
