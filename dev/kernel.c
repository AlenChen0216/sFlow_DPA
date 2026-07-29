/*
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * DPA receive path for IPv4/UDP destination port 8888. Hardware steering is
 * installed by dpu/sflow_dpa_comch_server.c; this file only sees matching
 * packets.
 */

#include <doca_dpa_dev.h>
#include <doca_dpa_dev_buf.h>
#include <doca_dpa_dev_verbs.h>
#include <dpaintrin.h>

#include "../common/sflow_dpa_common.h"

/*
 * DPACC device compilation does not provide the host libc headers. Keep the
 * DPA data path self-contained instead of depending on memset()/memcpy().
 */
static inline void fill_bytes(uint8_t *destination,
			      uint8_t value,
			      uint32_t length)
{
	uint32_t i;

	for (i = 0; i < length; ++i)
		destination[i] = value;
}

static inline void copy_bytes(uint8_t *destination,
			      const uint8_t *source,
			      uint32_t length)
{
	uint32_t i;

	for (i = 0; i < length; ++i)
		destination[i] = source[i];
}

/*
 * TODO(packet-modification): Implement your packet transformation here.
 *
 * "packet" points to a host-owned, 64-byte-aligned registered receive slot.
 * The packet includes the Ethernet header. "packet_length" is the actual byte
 * count reported by the receive completion and is at most
 * SFLOW_MAX_PACKET_SIZE.
 *
 * If you change IPv4/UDP headers or payload bytes, also update the relevant
 * length/checksum fields. The current project is receive-only; changing these
 * bytes does not transmit the packet back to the network.
 */
static inline void modify_packet(uint8_t *packet, uint32_t packet_length)
{
	uint8_t seg_cnt = 0;
	while (packet_length >= UDP_OFFSET + SEG_OFFSET * (seg_cnt + 1)){
		uint8_t val = 'a'+seg_cnt;
		fill_bytes(packet + UDP_OFFSET + SEG_OFFSET * seg_cnt, val, SEG_OFFSET);
		seg_cnt++;
	}
}

static inline uint32_t hash_key(const char *key, size_t key_length)
{
	uint32_t hash = 0;
	for(size_t i = 0 ; i < key_length; i++){
		hash += key[i];
	}
	return hash;
}

/*
 * TODO(data-storage): Extract and store your dedicated data here.
 *
 * Write at most SFLOW_DEDICATED_DATA_CAPACITY bytes to
 * output->dedicated_data and set output->dedicated_data_length to the number
 * of valid bytes. The host prints exactly that range after the receive batch.
 *
 * The skeleton deliberately stores no packet bytes. It only publishes the
 * packet count, length, and timestamp, proving that the registration and
 * DPA-to-host visibility path works before application-specific logic exists.
 */
static inline void store_dedicated_data(const uint8_t *packet,
					uint32_t packet_length,
					struct sflow_dedicated_output *output)
{
	output->dedicated_data_length = packet_length;
	uint8_t seg_cnt = 0;
	while (packet_length >= UDP_OFFSET + SEG_OFFSET * (seg_cnt + 1)){
		uint32_t hash;
		char key[128];
		copy_bytes((uint8_t *)key,
			   packet + UDP_OFFSET + SEG_OFFSET * seg_cnt,
			   SEG_OFFSET);
		key[SEG_OFFSET] = '\0';
		hash = hash_key(key, 8);
		copy_bytes(
			(uint8_t *)output
				->dedicated_data[hash % SFLOW_DEDICATED_DATA_CAPACITY]
				.key,
			(const uint8_t *)key,
			SEG_OFFSET + 1);
		output->dedicated_data[hash % SFLOW_DEDICATED_DATA_CAPACITY].data.cnt++;
		seg_cnt++;
	}
}

/*
 * Post a finite receive batch, process every completion, and publish results
 * into host memory. A finite RPC keeps the registration skeleton testable; a
 * production daemon can later move the same body to a DPA thread/event model.
 */
__dpa_rpc__ uint64_t sflow_receive_rpc(doca_dpa_dev_verbs_eth_rq_t rq_handle,
				       doca_dpa_dev_completion_t completion_handle,
				       doca_dpa_dev_mmap_t host_mmap_handle,
				       uint64_t host_memory_address,
				       uint32_t receive_mkey,
				       uint32_t packet_count)
{
	doca_dpa_dev_uintptr_t external_base;
	struct sflow_dedicated_output *output;
	struct doca_dpa_dev_verbs_recv_wr recv_wr;
	struct doca_dpa_dev_verbs_sge sge;
	doca_dpa_dev_completion_element_t completion;
	uint32_t i;

	if (packet_count == 0 || packet_count > SFLOW_RX_QUEUE_DEPTH)
		return 1;

	external_base = doca_dpa_dev_mmap_get_external_ptr(host_mmap_handle,
							   host_memory_address);
	if (external_base == 0)
		return 2;

	output = (struct sflow_dedicated_output *)(external_base + SFLOW_OUTPUT_OFFSET);

	for (i = 0; i < packet_count; ++i) {
		sge.addr = host_memory_address + SFLOW_RX_SLOTS_OFFSET +
			   ((uint64_t)i * SFLOW_MAX_PACKET_SIZE);
		sge.length = SFLOW_MAX_PACKET_SIZE;
		sge.lkey = receive_mkey;

		doca_dpa_dev_verbs_recv_wr_set_sg_list(&recv_wr, &sge);
		doca_dpa_dev_verbs_recv_wr_set_sg_num_sge(&recv_wr, 1);
		doca_dpa_dev_verbs_eth_rq_post_recv_wr(rq_handle, &recv_wr);
	}
	doca_dpa_dev_verbs_eth_rq_commit_recv(rq_handle);

	for (i = 0; i < packet_count; ++i) {
		uint32_t wqe_counter;
		uint32_t slot;
		uint32_t received_bytes;
		uint8_t *packet;

		while (!doca_dpa_dev_get_completion(completion_handle, &completion))
			;

		if (doca_dpa_dev_get_completion_type(completion) ==
		    DOCA_DPA_DEV_COMP_RECV_ERR) {
			doca_dpa_dev_completion_ack(completion_handle, i + 1);
			return 3;
		}

		wqe_counter = doca_dpa_dev_completion_element_get_wqe_counter(completion);
		slot = wqe_counter % packet_count;
		received_bytes =
			doca_dpa_dev_completion_element_get_received_bytes(completion);
		if (received_bytes > SFLOW_MAX_PACKET_SIZE)
			received_bytes = SFLOW_MAX_PACKET_SIZE;

		packet = (uint8_t *)(external_base + SFLOW_RX_SLOTS_OFFSET +
				     ((uint64_t)slot * SFLOW_MAX_PACKET_SIZE));

		/* The NIC produced packet bytes; invalidate window reads before DPA loads. */
		__dpa_thread_window_read_inv();

		modify_packet(packet, received_bytes);

		output->abi_version = SFLOW_OUTPUT_ABI_VERSION;
		output->packets_received = i + 1;
		output->last_packet_length = received_bytes;
		output->last_packet_timestamp =
			doca_dpa_dev_completion_element_get_timestamp(completion);
		store_dedicated_data(packet, received_bytes, output);

		/* Make packet edits and the output record visible to the host CPU. */
		__dpa_thread_window_writeback();
	}

	doca_dpa_dev_completion_ack(completion_handle, packet_count);
	return 0;
}
