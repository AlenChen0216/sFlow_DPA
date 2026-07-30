/*
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Control and immediate-data protocol shared by the Comch producer/consumer
 * sFlow output service.
 */

#ifndef SFLOW_COMCH_PRODUCER_CONSUMER_PROTOCOL_H
#define SFLOW_COMCH_PRODUCER_CONSUMER_PROTOCOL_H

#include <stdint.h>

#define SFLOW_COMCH_PC_SERVER_NAME "sflow-dpa-output-pc"
#define SFLOW_COMCH_PC_PROTOCOL_MAGIC UINT32_C(0x53465043) /* "SFPC" */
#define SFLOW_COMCH_PC_PROTOCOL_VERSION 1U
#define SFLOW_COMCH_PC_RECV_QUEUE_SIZE 8U
#define SFLOW_COMCH_PC_CONTROL_TASK_COUNT 1U
#define SFLOW_COMCH_PC_DATA_TASK_COUNT 1U
#define SFLOW_COMCH_PC_ERROR_TEXT_MAX 128U

enum sflow_comch_pc_message_type {
	SFLOW_COMCH_PC_MSG_GET_OUTPUT = 1,
	SFLOW_COMCH_PC_MSG_OUTPUT_CHUNK = 2,
	SFLOW_COMCH_PC_MSG_ERROR = 3,
};

enum sflow_comch_pc_status {
	SFLOW_COMCH_PC_STATUS_OK = 0,
	SFLOW_COMCH_PC_STATUS_BAD_REQUEST = 1,
	SFLOW_COMCH_PC_STATUS_BUSY = 2,
	SFLOW_COMCH_PC_STATUS_INTERNAL_ERROR = 3,
	SFLOW_COMCH_PC_STATUS_NO_CONSUMER = 4,
};

/*
 * All fields use network byte order.
 *
 * GET_OUTPUT is a control-channel message. total_length and
 * output_abi_version describe the receive buffer expected by the client;
 * payload_length advertises the largest data-path buffer the consumer can
 * accept.
 *
 * OUTPUT_CHUNK is carried as producer immediate data while the associated
 * doca_buf contains the payload bytes.
 */
struct sflow_comch_pc_header {
	uint32_t magic;
	uint16_t protocol_version;
	uint16_t message_type;
	uint32_t request_id;
	uint32_t status;
	uint32_t output_abi_version;
	uint32_t total_length;
	uint32_t offset;
	uint32_t payload_length;
} __attribute__((packed));

_Static_assert(sizeof(struct sflow_comch_pc_header) == 32U,
	       "Comch producer/consumer protocol header must remain 32 bytes");

#endif /* SFLOW_COMCH_PRODUCER_CONSUMER_PROTOCOL_H */
