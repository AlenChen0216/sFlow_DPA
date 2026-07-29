/*
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Wire protocol shared by the BlueField Arm Comch server and x86 client.
 */

#ifndef SFLOW_COMCH_PROTOCOL_H
#define SFLOW_COMCH_PROTOCOL_H

#include <stdint.h>

#define SFLOW_COMCH_SERVER_NAME "sflow-dpa-output"
#define SFLOW_COMCH_PROTOCOL_MAGIC UINT32_C(0x53464c57) /* "SFLW" */
#define SFLOW_COMCH_PROTOCOL_VERSION 1U
#define SFLOW_COMCH_RECV_QUEUE_SIZE 8U
#define SFLOW_COMCH_SEND_TASK_COUNT 1U

enum sflow_comch_message_type {
	SFLOW_COMCH_MSG_GET_OUTPUT = 1,
	SFLOW_COMCH_MSG_OUTPUT_CHUNK = 2,
	SFLOW_COMCH_MSG_ERROR = 3,
};

enum sflow_comch_status {
	SFLOW_COMCH_STATUS_OK = 0,
	SFLOW_COMCH_STATUS_BAD_REQUEST = 1,
	SFLOW_COMCH_STATUS_BUSY = 2,
	SFLOW_COMCH_STATUS_INTERNAL_ERROR = 3,
};

/*
 * All header fields use network byte order. OUTPUT_CHUNK payload bytes are a
 * byte-for-byte snapshot of struct sflow_dedicated_output. BlueField Arm and
 * supported x86 hosts are little-endian and compile the same common header;
 * output_abi_version and total_length reject incompatible layouts.
 */
struct sflow_comch_header {
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

_Static_assert(sizeof(struct sflow_comch_header) == 32U,
	       "Comch protocol header must remain 32 bytes");

#endif /* SFLOW_COMCH_PROTOCOL_H */
