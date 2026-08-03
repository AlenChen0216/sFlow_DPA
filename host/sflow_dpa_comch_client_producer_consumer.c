/*
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * x86 Comch consumer for retrieving sflow_dedicated_output from a BlueField
 * Arm Comch producer.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_comch.h>
#include <doca_comch_consumer.h>
#include <doca_ctx.h>
#include <doca_dev.h>
#include <doca_error.h>
#include <doca_log.h>
#include <doca_mmap.h>
#include <doca_pe.h>
#include <doca_types.h>

#include "../common/sflow_comch_producer_consumer_protocol.h"
#include "../common/sflow_dpa_common.h"

struct comch_consumer_client_state {
	struct doca_dev *dev;
	struct doca_pe *control_pe;
	struct doca_pe *consumer_pe;
	struct doca_comch_client *client;
	struct doca_comch_connection *connection;
	struct doca_comch_consumer *consumer;
	struct doca_mmap *consumer_mmap;
	struct doca_buf_inventory *buffer_inventory;

	struct sflow_comch_pc_header request;
	struct sflow_dedicated_output output;
	uint32_t max_consumer_buffer;
	uint32_t request_id;
	uint32_t bytes_received;

	doca_error_t result;
	bool client_started;
	bool client_idle;
	bool connection_ready;
	bool consumer_started;
	bool consumer_running;
	bool consumer_idle;
	bool request_submitted;
	bool receive_active;
	bool response_received;
};

static volatile sig_atomic_t stop_requested;

static void log_doca_error(const char *operation, doca_error_t status)
{
	fprintf(stderr, "%s: %s\n", operation, doca_error_get_descr(status));
}

static void handle_stop_signal(int signal_number)
{
	(void)signal_number;
	stop_requested = 1;
}

static doca_error_t open_device_by_pci(const char *pci_address,
				       struct doca_dev **dev)
{
	struct doca_devinfo **device_list = NULL;
	uint32_t device_count = 0;
	doca_error_t status;
	uint32_t i;

	*dev = NULL;
	status = doca_devinfo_create_list(&device_list, &device_count);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_devinfo_create_list failed", status);
		return status;
	}

	status = DOCA_ERROR_NOT_FOUND;
	for (i = 0; i < device_count; ++i) {
		uint8_t address_matches = 0;

		if (doca_devinfo_is_equal_pci_addr(device_list[i],
						  pci_address,
						  &address_matches) != DOCA_SUCCESS ||
		    address_matches == 0)
			continue;

		status = doca_comch_cap_client_is_supported(device_list[i]);
		if (status != DOCA_SUCCESS)
			break;
		status = doca_comch_consumer_cap_is_supported(device_list[i]);
		if (status != DOCA_SUCCESS)
			break;
		status = doca_dev_open(device_list[i], dev);
		break;
	}

	(void)doca_devinfo_destroy_list(device_list);
	if (status != DOCA_SUCCESS) {
		fprintf(stderr,
			"Unable to open Comch consumer device '%s': %s\n",
			pci_address,
			doca_error_get_descr(status));
	}
	return status;
}

static void encode_request(struct sflow_comch_pc_header *request,
			   uint32_t request_id,
			   uint32_t max_consumer_buffer)
{
	memset(request, 0, sizeof(*request));
	request->magic = htonl(SFLOW_COMCH_PC_PROTOCOL_MAGIC);
	request->protocol_version = htons(SFLOW_COMCH_PC_PROTOCOL_VERSION);
	request->message_type = htons(SFLOW_COMCH_PC_MSG_GET_OUTPUT);
	request->request_id = htonl(request_id);
	request->status = htonl(SFLOW_COMCH_PC_STATUS_OK);
	request->output_abi_version = htonl(SFLOW_OUTPUT_ABI_VERSION);
	request->total_length =
		htonl((uint32_t)sizeof(struct sflow_dedicated_output));
	request->payload_length = htonl(max_consumer_buffer);
}

static void stop_control_client(struct comch_consumer_client_state *state)
{
	doca_error_t status;

	if (!state->client_started || state->client_idle)
		return;

	status = doca_ctx_stop(doca_comch_client_as_ctx(state->client));
	if (status != DOCA_SUCCESS && status != DOCA_ERROR_IN_PROGRESS)
		log_doca_error("doca_ctx_stop(Comch client) failed", status);
}

static void stop_consumer(struct comch_consumer_client_state *state)
{
	doca_error_t status;

	if (!state->consumer_started || state->consumer_idle)
		return;

	status = doca_ctx_stop(doca_comch_consumer_as_ctx(state->consumer));
	if (status != DOCA_SUCCESS && status != DOCA_ERROR_IN_PROGRESS)
		log_doca_error("doca_ctx_stop(Comch consumer) failed", status);
}

static void fail_data_path(struct comch_consumer_client_state *state,
			   const char *operation,
			   doca_error_t status)
{
	if (state->result == DOCA_SUCCESS)
		state->result = status;
	if (operation != NULL)
		log_doca_error(operation, status);

	if (state->consumer_started && !state->consumer_idle)
		stop_consumer(state);
	else
		stop_control_client(state);
}

static void control_send_completion_callback(
	struct doca_comch_task_send *send_task,
	union doca_data task_user_data,
	union doca_data context_user_data)
{
	(void)task_user_data;
	(void)context_user_data;
	doca_task_free(doca_comch_task_send_as_task(send_task));
}

static void control_send_error_callback(
	struct doca_comch_task_send *send_task,
	union doca_data task_user_data,
	union doca_data context_user_data)
{
	struct comch_consumer_client_state *state = context_user_data.ptr;
	doca_error_t status =
		doca_task_get_status(doca_comch_task_send_as_task(send_task));

	(void)task_user_data;
	doca_task_free(doca_comch_task_send_as_task(send_task));
	fail_data_path(state, "Comch GET_OUTPUT request send failed", status);
}

static doca_error_t submit_get_output_request(
	struct comch_consumer_client_state *state)
{
	struct doca_comch_task_send *send_task = NULL;
	struct doca_task *task;
	doca_error_t status;

	encode_request(&state->request,
		       state->request_id,
		       state->max_consumer_buffer);
	status = doca_comch_client_task_send_alloc_init(
		state->client,
		state->connection,
		&state->request,
		sizeof(state->request),
		&send_task);
	if (status != DOCA_SUCCESS)
		return status;

	task = doca_comch_task_send_as_task(send_task);
	status = doca_task_submit(task);
	if (status != DOCA_SUCCESS) {
		doca_task_free(task);
		return status;
	}

	state->request_submitted = true;
	return DOCA_SUCCESS;
}

static bool decode_and_validate_chunk_header(
	const uint8_t *immediate_data,
	uint32_t immediate_data_length,
	uint32_t received_length,
	const struct comch_consumer_client_state *state,
	struct sflow_comch_pc_header *header)
{
	if (immediate_data == NULL ||
	    immediate_data_length != sizeof(*header))
		return false;

	memcpy(header, immediate_data, sizeof(*header));
	return ntohl(header->magic) == SFLOW_COMCH_PC_PROTOCOL_MAGIC &&
	       ntohs(header->protocol_version) ==
		       SFLOW_COMCH_PC_PROTOCOL_VERSION &&
	       ntohs(header->message_type) ==
		       SFLOW_COMCH_PC_MSG_OUTPUT_CHUNK &&
	       ntohl(header->request_id) == state->request_id &&
	       ntohl(header->status) == SFLOW_COMCH_PC_STATUS_OK &&
	       ntohl(header->output_abi_version) ==
		       SFLOW_OUTPUT_ABI_VERSION &&
	       ntohl(header->total_length) == sizeof(state->output) &&
	       ntohl(header->offset) == state->bytes_received &&
	       ntohl(header->payload_length) == received_length &&
	       received_length != 0 &&
	       received_length <= sizeof(state->output) -
					  state->bytes_received;
}

static doca_error_t post_next_receive(
	struct comch_consumer_client_state *state)
{
	struct doca_comch_consumer_task_post_recv *receive_task = NULL;
	struct doca_task *task;
	struct doca_buf *buffer = NULL;
	uint32_t remaining;
	uint32_t buffer_length;
	doca_error_t status;

	if (state->receive_active ||
	    state->bytes_received >= sizeof(state->output))
		return DOCA_ERROR_BAD_STATE;

	remaining = (uint32_t)sizeof(state->output) - state->bytes_received;
	buffer_length = remaining < state->max_consumer_buffer ?
				remaining :
				state->max_consumer_buffer;
	status = doca_buf_inventory_buf_get_by_addr(
		state->buffer_inventory,
		state->consumer_mmap,
		(uint8_t *)&state->output + state->bytes_received,
		buffer_length,
		&buffer);
	if (status != DOCA_SUCCESS)
		return status;

	status = doca_comch_consumer_task_post_recv_alloc_init(
		state->consumer,
		buffer,
		&receive_task);
	if (status != DOCA_SUCCESS) {
		(void)doca_buf_dec_refcount(buffer, NULL);
		return status;
	}

	task = doca_comch_consumer_task_post_recv_as_task(receive_task);
	status = doca_task_submit(task);
	if (status != DOCA_SUCCESS) {
		doca_task_free(task);
		(void)doca_buf_dec_refcount(buffer, NULL);
		return status;
	}

	state->receive_active = true;
	return DOCA_SUCCESS;
}

static void consumer_receive_completion_callback(
	struct doca_comch_consumer_task_post_recv *receive_task,
	union doca_data task_user_data,
	union doca_data context_user_data)
{
	struct comch_consumer_client_state *state = context_user_data.ptr;
	struct doca_buf *buffer =
		doca_comch_consumer_task_post_recv_get_buf(receive_task);
	const uint8_t *immediate_data;
	struct sflow_comch_pc_header header;
	void *received_data = NULL;
	size_t received_length = 0;
	uint32_t immediate_data_length;
	doca_error_t status;
	bool valid_header;

	(void)task_user_data;
	state->receive_active = false;

	status = doca_buf_get_data(buffer, &received_data);
	if (status == DOCA_SUCCESS)
		status = doca_buf_get_data_len(buffer, &received_length);
	immediate_data =
		doca_comch_consumer_task_post_recv_get_imm_data(receive_task);
	immediate_data_length =
		doca_comch_consumer_task_post_recv_get_imm_data_len(receive_task);

	valid_header =
		status == DOCA_SUCCESS &&
		received_length <= UINT32_MAX &&
		received_data == (uint8_t *)&state->output +
					 state->bytes_received &&
		decode_and_validate_chunk_header(immediate_data,
						 immediate_data_length,
						 (uint32_t)received_length,
						 state,
						 &header);

	(void)doca_buf_dec_refcount(buffer, NULL);
	doca_task_free(
		doca_comch_consumer_task_post_recv_as_task(receive_task));

	if (!valid_header) {
		fprintf(stderr,
			"Received an invalid or out-of-order Comch producer chunk\n");
		fail_data_path(state,
			       NULL,
			       status == DOCA_SUCCESS ?
				       DOCA_ERROR_INVALID_VALUE :
				       status);
		return;
	}

	state->bytes_received += (uint32_t)received_length;
	if (state->bytes_received == sizeof(state->output)) {
		state->response_received = true;
		state->result = DOCA_SUCCESS;
		stop_consumer(state);
		return;
	}

	status = post_next_receive(state);
	if (status != DOCA_SUCCESS)
		fail_data_path(state,
			       "Failed to post next Comch consumer buffer",
			       status);
}

static void consumer_receive_error_callback(
	struct doca_comch_consumer_task_post_recv *receive_task,
	union doca_data task_user_data,
	union doca_data context_user_data)
{
	struct comch_consumer_client_state *state = context_user_data.ptr;
	struct doca_buf *buffer =
		doca_comch_consumer_task_post_recv_get_buf(receive_task);
	doca_error_t status = doca_task_get_status(
		doca_comch_consumer_task_post_recv_as_task(receive_task));

	(void)task_user_data;
	state->receive_active = false;
	(void)doca_buf_dec_refcount(buffer, NULL);
	doca_task_free(
		doca_comch_consumer_task_post_recv_as_task(receive_task));

	if (!state->response_received)
		fail_data_path(state, "Comch consumer receive failed", status);
}

static void control_message_receive_callback(
	struct doca_comch_event_msg_recv *event,
	uint8_t *receive_buffer,
	uint32_t message_length,
	struct doca_comch_connection *connection)
{
	union doca_data connection_data =
		doca_comch_connection_get_user_data(connection);
	struct comch_consumer_client_state *state = connection_data.ptr;
	struct sflow_comch_pc_header header;
	uint32_t payload_length;
	uint32_t response_status;
	bool valid;

	(void)event;
	if (state == NULL)
		return;

	valid = message_length >= sizeof(header);
	if (valid) {
		memcpy(&header, receive_buffer, sizeof(header));
		payload_length = ntohl(header.payload_length);
		response_status = ntohl(header.status);
		valid =
			ntohl(header.magic) == SFLOW_COMCH_PC_PROTOCOL_MAGIC &&
			ntohs(header.protocol_version) ==
				SFLOW_COMCH_PC_PROTOCOL_VERSION &&
			ntohs(header.message_type) ==
				SFLOW_COMCH_PC_MSG_ERROR &&
			ntohl(header.request_id) == state->request_id &&
			message_length == sizeof(header) + payload_length;
	} else {
		payload_length = 0;
		response_status = SFLOW_COMCH_PC_STATUS_INTERNAL_ERROR;
	}

	if (!valid) {
		fprintf(stderr,
			"Received an invalid Comch producer/consumer control frame\n");
		fail_data_path(state, NULL, DOCA_ERROR_INVALID_VALUE);
		return;
	}

	fprintf(stderr,
		"Comch producer rejected request (status %" PRIu32
		"): %.*s\n",
		response_status,
		(int)payload_length,
		(const char *)receive_buffer + sizeof(header));
	fail_data_path(state, NULL, DOCA_ERROR_BAD_STATE);
}

static void control_context_state_changed_callback(
	const union doca_data user_data,
	struct doca_ctx *context,
	enum doca_ctx_states previous_state,
	enum doca_ctx_states next_state)
{
	struct comch_consumer_client_state *state = user_data.ptr;
	union doca_data connection_data = {0};
	doca_error_t status;

	(void)context;
	(void)previous_state;
	switch (next_state) {
	case DOCA_CTX_STATE_RUNNING:
		status = doca_comch_client_get_connection(state->client,
							  &state->connection);
		if (status != DOCA_SUCCESS) {
			fail_data_path(state,
				       "Failed to get Comch connection",
				       status);
			break;
		}
		connection_data.ptr = state;
		status = doca_comch_connection_set_user_data(
			state->connection,
			connection_data);
		if (status != DOCA_SUCCESS) {
			fail_data_path(state,
				       "Failed to set Comch connection data",
				       status);
			break;
		}
		state->connection_ready = true;
		break;
	case DOCA_CTX_STATE_IDLE:
		state->client_idle = true;
		break;
	default:
		break;
	}
}

static void consumer_context_state_changed_callback(
	const union doca_data user_data,
	struct doca_ctx *context,
	enum doca_ctx_states previous_state,
	enum doca_ctx_states next_state)
{
	struct comch_consumer_client_state *state = user_data.ptr;
	doca_error_t status;

	(void)context;
	switch (next_state) {
	case DOCA_CTX_STATE_RUNNING:
		state->consumer_running = true;
		status = post_next_receive(state);
		if (status == DOCA_SUCCESS)
			status = submit_get_output_request(state);
		if (status != DOCA_SUCCESS)
			fail_data_path(state,
				       "Failed to start Comch consumer transfer",
				       status);
		break;
	case DOCA_CTX_STATE_IDLE:
		state->consumer_running = false;
		state->consumer_idle = true;
		if (!state->response_received &&
		    state->result == DOCA_SUCCESS &&
		    previous_state != DOCA_CTX_STATE_STOPPING)
			state->result = DOCA_ERROR_UNEXPECTED;
		break;
	default:
		break;
	}
}

static doca_error_t create_control_client(
	const char *pci_address,
	struct comch_consumer_client_state *state)
{
	struct doca_ctx *context;
	union doca_data context_data = {0};
	uint32_t max_control_message_size;
	uint32_t max_immediate_data_length;
	uint32_t max_consumer_tasks;
	doca_error_t status;

	state->request_id = 1;
	state->result = DOCA_SUCCESS;

	status = open_device_by_pci(pci_address, &state->dev);
	if (status != DOCA_SUCCESS)
		return status;
	status = doca_comch_cap_get_max_msg_size(doca_dev_as_devinfo(state->dev),
						 &max_control_message_size);
	if (status != DOCA_SUCCESS)
		goto error;
	if (max_control_message_size < sizeof(struct sflow_comch_pc_header)) {
		status = DOCA_ERROR_NOT_SUPPORTED;
		goto error;
	}
	status = doca_comch_consumer_cap_get_max_buf_size(
		doca_dev_as_devinfo(state->dev),
		&state->max_consumer_buffer);
	if (status != DOCA_SUCCESS)
		goto error;
	if (state->max_consumer_buffer == 0) {
		status = DOCA_ERROR_NOT_SUPPORTED;
		goto error;
	}
	if (state->max_consumer_buffer > sizeof(state->output))
		state->max_consumer_buffer = sizeof(state->output);
	status = doca_comch_consumer_cap_get_max_imm_data_len(
		doca_dev_as_devinfo(state->dev),
		&max_immediate_data_length);
	if (status != DOCA_SUCCESS)
		goto error;
	if (max_immediate_data_length <
	    sizeof(struct sflow_comch_pc_header)) {
		status = DOCA_ERROR_NOT_SUPPORTED;
		goto error;
	}
	status = doca_comch_consumer_cap_get_max_num_tasks(
		doca_dev_as_devinfo(state->dev),
		&max_consumer_tasks);
	if (status != DOCA_SUCCESS)
		goto error;
	if (max_consumer_tasks < SFLOW_COMCH_PC_DATA_TASK_COUNT) {
		status = DOCA_ERROR_NOT_SUPPORTED;
		goto error;
	}

	status = doca_pe_create(&state->control_pe);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_client_create(state->dev,
					  SFLOW_COMCH_PC_SERVER_NAME,
					  &state->client);
	if (status != DOCA_SUCCESS)
		goto error;
	context = doca_comch_client_as_ctx(state->client);

	status = doca_pe_connect_ctx(state->control_pe, context);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_ctx_set_state_changed_cb(
		context,
		control_context_state_changed_callback);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_client_task_send_set_conf(
		state->client,
		control_send_completion_callback,
		control_send_error_callback,
		SFLOW_COMCH_PC_CONTROL_TASK_COUNT);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_client_event_msg_recv_register(
		state->client,
		control_message_receive_callback);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_client_set_max_msg_size(
		state->client,
		max_control_message_size);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_client_set_recv_queue_size(
		state->client,
		SFLOW_COMCH_PC_RECV_QUEUE_SIZE);
	if (status != DOCA_SUCCESS)
		goto error;

	context_data.ptr = state;
	status = doca_ctx_set_user_data(context, context_data);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_ctx_start(context);
	if (status != DOCA_ERROR_IN_PROGRESS && status != DOCA_SUCCESS)
		goto error;
	state->client_started = true;
	return DOCA_SUCCESS;

error:
	log_doca_error("Failed to create Comch consumer client", status);
	return status;
}

static doca_error_t create_consumer_data_path(
	struct comch_consumer_client_state *state)
{
	struct doca_ctx *context;
	union doca_data context_data = {0};
	doca_error_t status;

	status = doca_buf_inventory_create(1, &state->buffer_inventory);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_buf_inventory_start(state->buffer_inventory);
	if (status != DOCA_SUCCESS)
		goto error;

	status = doca_mmap_create(&state->consumer_mmap);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_mmap_add_dev(state->consumer_mmap, state->dev);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_mmap_set_permissions(state->consumer_mmap,
					   DOCA_ACCESS_FLAG_PCI_READ_WRITE);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_mmap_set_memrange(state->consumer_mmap,
					&state->output,
					sizeof(state->output));
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_mmap_start(state->consumer_mmap);
	if (status != DOCA_SUCCESS)
		goto error;

	status = doca_pe_create(&state->consumer_pe);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_consumer_create(state->connection,
					    state->consumer_mmap,
					    &state->consumer);
	if (status != DOCA_SUCCESS)
		goto error;
	context = doca_comch_consumer_as_ctx(state->consumer);

	status = doca_pe_connect_ctx(state->consumer_pe, context);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_ctx_set_state_changed_cb(
		context,
		consumer_context_state_changed_callback);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_consumer_set_imm_data_len(
		state->consumer,
		sizeof(struct sflow_comch_pc_header));
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_consumer_task_post_recv_set_conf(
		state->consumer,
		consumer_receive_completion_callback,
		consumer_receive_error_callback,
		SFLOW_COMCH_PC_DATA_TASK_COUNT);
	if (status != DOCA_SUCCESS)
		goto error;

	context_data.ptr = state;
	status = doca_ctx_set_user_data(context, context_data);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_ctx_start(context);
	if (status != DOCA_ERROR_IN_PROGRESS && status != DOCA_SUCCESS)
		goto error;
	state->consumer_started = true;
	return DOCA_SUCCESS;

error:
	log_doca_error("Failed to create Comch consumer data path", status);
	return status;
}

static doca_error_t destroy_consumer_data_path(
	struct comch_consumer_client_state *state)
{
	doca_error_t result = DOCA_SUCCESS;
	doca_error_t status;

	if (state->consumer != NULL) {
		status = doca_comch_consumer_destroy(state->consumer);
		if (status != DOCA_SUCCESS) {
			log_doca_error("doca_comch_consumer_destroy failed", status);
			result = status;
		} else {
			state->consumer = NULL;
			state->consumer_started = false;
		}
	}
	if (state->consumer_pe != NULL) {
		status = doca_pe_destroy(state->consumer_pe);
		if (status != DOCA_SUCCESS) {
			log_doca_error("doca_pe_destroy(consumer) failed", status);
			if (result == DOCA_SUCCESS)
				result = status;
		} else {
			state->consumer_pe = NULL;
		}
	}
	if (state->consumer_mmap != NULL) {
		status = doca_mmap_destroy(state->consumer_mmap);
		if (status != DOCA_SUCCESS) {
			log_doca_error("doca_mmap_destroy(consumer) failed", status);
			if (result == DOCA_SUCCESS)
				result = status;
		} else {
			state->consumer_mmap = NULL;
		}
	}
	if (state->buffer_inventory != NULL) {
		status = doca_buf_inventory_destroy(state->buffer_inventory);
		if (status != DOCA_SUCCESS) {
			log_doca_error("doca_buf_inventory_destroy failed", status);
			if (result == DOCA_SUCCESS)
				result = status;
		} else {
			state->buffer_inventory = NULL;
		}
	}
	return result;
}

static void progress_client(struct comch_consumer_client_state *state)
{
	(void)doca_pe_progress(state->control_pe);
	if (state->consumer_pe != NULL)
		(void)doca_pe_progress(state->consumer_pe);
}

static doca_error_t run_comch_consumer_client(
	struct comch_consumer_client_state *state)
{
	const struct timespec idle_delay = {
		.tv_sec = 0,
		.tv_nsec = 10000,
	};
	doca_error_t status;

	while (!stop_requested && !state->connection_ready &&
	       !state->client_idle) {
		if (doca_pe_progress(state->control_pe) == 0)
			(void)nanosleep(&idle_delay, NULL);
	}
	if (stop_requested || state->client_idle) {
		if (stop_requested)
			state->result = DOCA_ERROR_OPERATING_SYSTEM;
		else if (state->result == DOCA_SUCCESS)
			state->result = DOCA_ERROR_UNEXPECTED;
		stop_control_client(state);
		goto stop_control;
	}

	status = create_consumer_data_path(state);
	if (status != DOCA_SUCCESS) {
		state->result = status;
		stop_control_client(state);
		goto stop_control;
	}

	while (!stop_requested && !state->response_received &&
	       !state->client_idle && !state->consumer_idle) {
		uint32_t progress = doca_pe_progress(state->control_pe);

		progress += doca_pe_progress(state->consumer_pe);
		if (progress == 0)
			(void)nanosleep(&idle_delay, NULL);
	}

	if (stop_requested && state->result == DOCA_SUCCESS)
		state->result = DOCA_ERROR_OPERATING_SYSTEM;
	if (state->client_idle && !state->response_received &&
	    state->result == DOCA_SUCCESS)
		state->result = DOCA_ERROR_UNEXPECTED;
	if (!state->consumer_idle)
		stop_consumer(state);
	while (!state->consumer_idle) {
		progress_client(state);
	}

	status = destroy_consumer_data_path(state);
	if (status != DOCA_SUCCESS && state->result == DOCA_SUCCESS)
		state->result = status;
	if (!state->client_idle)
		stop_control_client(state);

stop_control:
	while (!state->client_idle)
		(void)doca_pe_progress(state->control_pe);

	if (!state->response_received && state->result == DOCA_SUCCESS)
		return DOCA_ERROR_UNEXPECTED;
	return state->result;
}

static void stop_and_destroy_client(
	struct comch_consumer_client_state *state)
{
	doca_error_t status;

	if (state == NULL)
		return;

	if (state->consumer != NULL) {
		if (state->consumer_started && !state->consumer_idle) {
			stop_consumer(state);
			while (!state->consumer_idle)
				progress_client(state);
		}
		(void)destroy_consumer_data_path(state);
	}
	if (state->client != NULL) {
		if (state->client_started && !state->client_idle) {
			stop_control_client(state);
			while (!state->client_idle)
				(void)doca_pe_progress(state->control_pe);
		}
		status = doca_comch_client_destroy(state->client);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_comch_client_destroy failed", status);
	}
	if (state->control_pe != NULL) {
		status = doca_pe_destroy(state->control_pe);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_pe_destroy(control) failed", status);
	}
	if (state->dev != NULL) {
		status = doca_dev_close(state->dev);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_dev_close failed", status);
	}
}

static int validate_output(const struct sflow_dedicated_output *output)
{
	if (output->abi_version != SFLOW_OUTPUT_ABI_VERSION) {
		fprintf(stderr,
			"Invalid output ABI version: got %" PRIu32
			", expected %u\n",
			output->abi_version,
			SFLOW_OUTPUT_ABI_VERSION);
		return -1;
	}
	if (output->last_packet_length > SFLOW_MAX_PACKET_SIZE) {
		fprintf(stderr,
			"Invalid last packet length: %" PRIu32 "\n",
			output->last_packet_length);
		return -1;
	}
	if (output->dedicated_data_count > SFLOW_DEDICATED_DATA_CAPACITY) {
		fprintf(stderr,
			"Invalid dedicated data count: %" PRIu32 "\n",
			output->dedicated_data_count);
		return -1;
	}
	return 0;
}

static void print_output(const struct sflow_dedicated_output *output)
{
	uint32_t entries_printed = 0;
	uint32_t i;

	printf("DPA output:\n");
	printf("  ABI version:           %" PRIu32 "\n", output->abi_version);
	printf("  kernel status:         %" PRIu64 "\n", output->kernel_status);
	printf("  packets received:      %" PRIu64 "\n",
	       output->packets_received);
	printf("  last packet length:    %" PRIu32 "\n",
	       output->last_packet_length);
	printf("  last packet timestamp: %" PRIu64 "\n",
	       output->last_packet_timestamp);
	printf("  dedicated data count:  %" PRIu32 "\n",
	       output->dedicated_data_count);
	printf("  dedicated data:");
	for (i = 0;
	     i < SFLOW_DEDICATED_DATA_CAPACITY &&
	     entries_printed < output->dedicated_data_count;
	     ++i) {
		const struct hash_entry *entry = &output->dedicated_data[i];
		size_t key_length;

		if (entry->data.cnt == 0)
			continue;
		entries_printed++;
		key_length = strnlen(entry->key, sizeof(entry->key));
		printf("\n    key: %.*s, count: %" PRIu32,
		       (int)key_length,
		       entry->key,
		       entry->data.cnt);
	}
	printf("\n");
}

int main(int argc, char **argv)
{
	struct comch_consumer_client_state *state = NULL;
	struct doca_log_backend *sdk_log = NULL;
	struct timespec start_time;
	struct timespec end_time;
	uint64_t elapsed_ns;
	doca_error_t status;
	int exit_status = EXIT_FAILURE;

	if (argc != 2) {
		fprintf(stderr,
			"Usage: %s <BlueField-PF-pci-address>\n",
			argv[0]);
		return EXIT_FAILURE;
	}
	if (geteuid() != 0) {
		fprintf(stderr, "This program must run with root privileges\n");
		return EXIT_FAILURE;
	}
#if !defined(__x86_64__)
	fprintf(stderr,
		"This Comch consumer client must be built and run on x86_64\n");
	return EXIT_FAILURE;
#endif

	if (signal(SIGINT, handle_stop_signal) == SIG_ERR ||
	    signal(SIGTERM, handle_stop_signal) == SIG_ERR) {
		fprintf(stderr,
			"Failed to install signal handlers: %s\n",
			strerror(errno));
		return EXIT_FAILURE;
	}
	status = doca_log_backend_create_with_file_sdk(stderr, &sdk_log);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_log_backend_create_with_file_sdk failed",
			       status);
		return EXIT_FAILURE;
	}
	status = doca_log_backend_set_sdk_level(sdk_log,
						DOCA_LOG_LEVEL_WARNING);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_log_backend_set_sdk_level failed", status);
		return EXIT_FAILURE;
	}

	state = calloc(1, sizeof(*state));
	if (state == NULL) {
		fprintf(stderr,
			"Failed to allocate %zu-byte Comch consumer state\n",
			sizeof(*state));
		return EXIT_FAILURE;
	}

	printf("Requesting sflow_dedicated_output from Comch producer '%s'...\n",
	       SFLOW_COMCH_PC_SERVER_NAME);
	status = create_control_client(argv[1], state);
	if (status != DOCA_SUCCESS)
		goto cleanup;

	(void)clock_gettime(CLOCK_MONOTONIC, &start_time);
	status = run_comch_consumer_client(state);
	if (status != DOCA_SUCCESS) {
		log_doca_error("Comch producer/consumer request failed", status);
		goto cleanup;
	}
	if (validate_output(&state->output) != 0)
		goto cleanup;

	(void)clock_gettime(CLOCK_MONOTONIC, &end_time);
	elapsed_ns =
		(uint64_t)(end_time.tv_sec - start_time.tv_sec) *
			UINT64_C(1000000000) +
		(uint64_t)(end_time.tv_nsec - start_time.tv_nsec);
	printf("Comch producer/consumer request completed successfully in %"
	       PRIu64 " ns\n",
	       elapsed_ns);
	print_output(&state->output);
	exit_status = EXIT_SUCCESS;

cleanup:
	stop_and_destroy_client(state);
	free(state);
	return exit_status;
}
