/*
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * x86 Comch client for retrieving sflow_dedicated_output from BlueField Arm.
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

#include <doca_comch.h>
#include <doca_ctx.h>
#include <doca_dev.h>
#include <doca_error.h>
#include <doca_log.h>
#include <doca_pe.h>

#include "../common/sflow_comch_protocol.h"
#include "../common/sflow_dpa_common.h"

struct comch_client_state {
	struct doca_dev *dev;
	struct doca_pe *pe;
	struct doca_comch_client *client;
	struct doca_comch_connection *connection;

	struct sflow_comch_header request;
	struct sflow_dedicated_output output;
	uint32_t request_id;
	uint32_t bytes_received;

	doca_error_t result;
	bool context_started;
	bool context_idle;
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
		status = doca_dev_open(device_list[i], dev);
		break;
	}

	(void)doca_devinfo_destroy_list(device_list);
	if (status != DOCA_SUCCESS) {
		fprintf(stderr,
			"Unable to open Comch client device '%s': %s\n",
			pci_address,
			doca_error_get_descr(status));
	}
	return status;
}

static void encode_request(struct sflow_comch_header *request,
			   uint32_t request_id)
{
	memset(request, 0, sizeof(*request));
	request->magic = htonl(SFLOW_COMCH_PROTOCOL_MAGIC);
	request->protocol_version = htons(SFLOW_COMCH_PROTOCOL_VERSION);
	request->message_type = htons(SFLOW_COMCH_MSG_GET_OUTPUT);
	request->request_id = htonl(request_id);
	request->status = htonl(SFLOW_COMCH_STATUS_OK);
}

static void stop_client(struct comch_client_state *state)
{
	doca_error_t status =
		doca_ctx_stop(doca_comch_client_as_ctx(state->client));

	if (status != DOCA_SUCCESS && status != DOCA_ERROR_IN_PROGRESS)
		log_doca_error("doca_ctx_stop(Comch client) failed", status);
}

static void send_task_completion_callback(struct doca_comch_task_send *send_task,
					  union doca_data task_user_data,
					  union doca_data context_user_data)
{
	(void)task_user_data;
	(void)context_user_data;
	doca_task_free(doca_comch_task_send_as_task(send_task));
}

static void send_task_error_callback(struct doca_comch_task_send *send_task,
				     union doca_data task_user_data,
				     union doca_data context_user_data)
{
	struct comch_client_state *state = context_user_data.ptr;

	(void)task_user_data;
	state->result =
		doca_task_get_status(doca_comch_task_send_as_task(send_task));
	log_doca_error("Comch request send failed", state->result);
	doca_task_free(doca_comch_task_send_as_task(send_task));
	stop_client(state);
}

static doca_error_t submit_get_output_request(struct comch_client_state *state)
{
	struct doca_comch_task_send *send_task = NULL;
	struct doca_task *task;
	union doca_data connection_data = {0};
	doca_error_t status;

	status = doca_comch_client_get_connection(state->client,
						  &state->connection);
	if (status != DOCA_SUCCESS)
		return status;

	connection_data.ptr = state;
	status = doca_comch_connection_set_user_data(state->connection,
						     connection_data);
	if (status != DOCA_SUCCESS)
		return status;

	encode_request(&state->request, state->request_id);
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
	if (status != DOCA_SUCCESS)
		doca_task_free(task);
	return status;
}

static bool decode_and_validate_header(const uint8_t *receive_buffer,
				       uint32_t message_length,
				       struct sflow_comch_header *header,
				       uint16_t *message_type,
				       uint32_t *status,
				       uint32_t *total_length,
				       uint32_t *offset,
				       uint32_t *payload_length,
				       uint32_t request_id)
{
	if (message_length < sizeof(*header))
		return false;
	memcpy(header, receive_buffer, sizeof(*header));

	*message_type = ntohs(header->message_type);
	*status = ntohl(header->status);
	*total_length = ntohl(header->total_length);
	*offset = ntohl(header->offset);
	*payload_length = ntohl(header->payload_length);

	return ntohl(header->magic) == SFLOW_COMCH_PROTOCOL_MAGIC &&
	       ntohs(header->protocol_version) ==
		       SFLOW_COMCH_PROTOCOL_VERSION &&
	       ntohl(header->request_id) == request_id &&
	       message_length == sizeof(*header) + *payload_length &&
	       *offset <= *total_length &&
	       *payload_length <= *total_length - *offset;
}

static void message_receive_callback(struct doca_comch_event_msg_recv *event,
				     uint8_t *receive_buffer,
				     uint32_t message_length,
				     struct doca_comch_connection *connection)
{
	union doca_data connection_data =
		doca_comch_connection_get_user_data(connection);
	struct comch_client_state *state = connection_data.ptr;
	struct sflow_comch_header header;
	uint16_t message_type = 0;
	uint32_t response_status = 0;
	uint32_t total_length = 0;
	uint32_t offset = 0;
	uint32_t payload_length = 0;
	const uint8_t *payload;
	bool header_valid;

	(void)event;
	header_valid = decode_and_validate_header(receive_buffer,
						  message_length,
						  &header,
						  &message_type,
						  &response_status,
						  &total_length,
						  &offset,
						  &payload_length,
						  state->request_id);
	if (!header_valid) {
		fprintf(stderr, "Received an invalid sFlow Comch frame\n");
		state->result = DOCA_ERROR_INVALID_VALUE;
		stop_client(state);
		return;
	}
	payload = receive_buffer + sizeof(header);

	if (message_type == SFLOW_COMCH_MSG_ERROR) {
		fprintf(stderr,
			"Comch server rejected request (status %" PRIu32
			"): %.*s\n",
			response_status,
			(int)payload_length,
			(const char *)payload);
		state->result = DOCA_ERROR_BAD_STATE;
		stop_client(state);
		return;
	}

	if (message_type != SFLOW_COMCH_MSG_OUTPUT_CHUNK ||
	    response_status != SFLOW_COMCH_STATUS_OK ||
	    ntohl(header.output_abi_version) != SFLOW_OUTPUT_ABI_VERSION ||
	    total_length != sizeof(state->output) ||
	    offset != state->bytes_received) {
		fprintf(stderr,
			"Received an incompatible or out-of-order sFlow Comch frame\n");
		state->result = DOCA_ERROR_INVALID_VALUE;
		stop_client(state);
		return;
	}

	memcpy((uint8_t *)&state->output + offset, payload, payload_length);
	state->bytes_received += payload_length;
	if (state->bytes_received == sizeof(state->output)) {
		state->response_received = true;
		state->result = DOCA_SUCCESS;
		stop_client(state);
	}
}

static void context_state_changed_callback(const union doca_data user_data,
					   struct doca_ctx *context,
					   enum doca_ctx_states previous_state,
					   enum doca_ctx_states next_state)
{
	struct comch_client_state *state = user_data.ptr;
	doca_error_t status;

	(void)context;
	(void)previous_state;
	switch (next_state) {
	case DOCA_CTX_STATE_RUNNING:
		status = submit_get_output_request(state);
		if (status != DOCA_SUCCESS) {
			state->result = status;
			log_doca_error("Failed to submit GET_OUTPUT request", status);
			stop_client(state);
		}
		break;
	case DOCA_CTX_STATE_IDLE:
		state->context_idle = true;
		break;
	default:
		break;
	}
}

static doca_error_t create_comch_client(const char *pci_address,
					struct comch_client_state *state)
{
	struct doca_ctx *context;
	union doca_data context_data = {0};
	uint32_t max_message_size;
	doca_error_t status;

	state->request_id = 1;
	state->result = DOCA_SUCCESS;

	status = open_device_by_pci(pci_address, &state->dev);
	if (status != DOCA_SUCCESS)
		return status;
	status = doca_comch_cap_get_max_msg_size(doca_dev_as_devinfo(state->dev),
						 &max_message_size);
	if (status != DOCA_SUCCESS)
		goto error;
	if (max_message_size <= sizeof(struct sflow_comch_header)) {
		status = DOCA_ERROR_NOT_SUPPORTED;
		goto error;
	}
	status = doca_pe_create(&state->pe);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_client_create(state->dev,
					  SFLOW_COMCH_SERVER_NAME,
					  &state->client);
	if (status != DOCA_SUCCESS)
		goto error;
	context = doca_comch_client_as_ctx(state->client);

	status = doca_pe_connect_ctx(state->pe, context);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_ctx_set_state_changed_cb(context,
					       context_state_changed_callback);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_client_task_send_set_conf(
		state->client,
		send_task_completion_callback,
		send_task_error_callback,
		SFLOW_COMCH_SEND_TASK_COUNT);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_client_event_msg_recv_register(
		state->client,
		message_receive_callback);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_client_set_max_msg_size(state->client,
						    max_message_size);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_client_set_recv_queue_size(
		state->client,
		SFLOW_COMCH_RECV_QUEUE_SIZE);
	if (status != DOCA_SUCCESS)
		goto error;

	context_data.ptr = state;
	status = doca_ctx_set_user_data(context, context_data);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_ctx_start(context);
	if (status != DOCA_ERROR_IN_PROGRESS && status != DOCA_SUCCESS)
		goto error;
	state->context_started = true;

	return DOCA_SUCCESS;

error:
	log_doca_error("Failed to create Comch client", status);
	return status;
}

static void stop_and_destroy_client(struct comch_client_state *state)
{
	doca_error_t status;

	if (state->client != NULL) {
		if (state->context_started && !state->context_idle) {
			stop_client(state);
			while (!state->context_idle)
				(void)doca_pe_progress(state->pe);
		}
		status = doca_comch_client_destroy(state->client);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_comch_client_destroy failed", status);
	}
	if (state->pe != NULL) {
		status = doca_pe_destroy(state->pe);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_pe_destroy failed", status);
	}
	if (state->dev != NULL) {
		status = doca_dev_close(state->dev);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_dev_close failed", status);
	}
}

static doca_error_t run_comch_client(struct comch_client_state *state)
{
	const struct timespec idle_delay = {
		.tv_sec = 0,
		.tv_nsec = 10000,
	};

	while (!stop_requested && !state->context_idle) {
		if (doca_pe_progress(state->pe) == 0)
			(void)nanosleep(&idle_delay, NULL);
	}
	if (stop_requested && !state->context_idle)
		stop_client(state);
	while (!state->context_idle)
		(void)doca_pe_progress(state->pe);

	if (stop_requested && !state->response_received)
		return DOCA_ERROR_OPERATING_SYSTEM;
	if (!state->response_received)
		return state->result == DOCA_SUCCESS ?
			       DOCA_ERROR_UNEXPECTED :
			       state->result;
	return state->result;
}

static int validate_output(const struct sflow_dedicated_output *output)
{
	if (output->abi_version != SFLOW_OUTPUT_ABI_VERSION) {
		fprintf(stderr,
			"Invalid output ABI version: got %" PRIu32 ", expected %u\n",
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
	return 0;
}

static void print_output(const struct sflow_dedicated_output *output)
{
	uint32_t reported_length = output->dedicated_data_length;
	uint32_t i;

	if (reported_length > SFLOW_DEDICATED_DATA_CAPACITY)
		reported_length = SFLOW_DEDICATED_DATA_CAPACITY;

	printf("DPA output:\n");
	printf("  ABI version:           %" PRIu32 "\n", output->abi_version);
	printf("  packets received:      %" PRIu64 "\n", output->packets_received);
	printf("  last packet length:    %" PRIu32 "\n",
	       output->last_packet_length);
	printf("  last packet timestamp: %" PRIu64 "\n",
	       output->last_packet_timestamp);
	printf("  dedicated data length: %" PRIu32 "\n", reported_length);
	printf("  dedicated data:");
	for (i = 0; i < SFLOW_DEDICATED_DATA_CAPACITY; ++i) {
		const struct hash_entry *entry = &output->dedicated_data[i];
		size_t key_length;

		if (entry->data.cnt == 0)
			continue;
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
	struct comch_client_state state = {0};
	struct doca_log_backend *sdk_log = NULL;
	doca_error_t status;
	int exit_status = EXIT_FAILURE;
	struct timespec start_time;
	struct timespec end_time;

	if (argc != 2) {
		fprintf(stderr, "Usage: %s <BlueField-PF-pci-address>\n", argv[0]);
		return EXIT_FAILURE;
	}
	if (geteuid() != 0) {
		fprintf(stderr, "This program must run with root privileges\n");
		return EXIT_FAILURE;
	}
#if !defined(__x86_64__)
	fprintf(stderr, "This Comch client must be built and run on x86_64\n");
	return EXIT_FAILURE;
#endif

	if (signal(SIGINT, handle_stop_signal) == SIG_ERR ||
	    signal(SIGTERM, handle_stop_signal) == SIG_ERR) {
		fprintf(stderr, "Failed to install signal handlers: %s\n",
			strerror(errno));
		return EXIT_FAILURE;
	}
	status = doca_log_backend_create_with_file_sdk(stderr, &sdk_log);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_log_backend_create_with_file_sdk failed", status);
		return EXIT_FAILURE;
	}
	status = doca_log_backend_set_sdk_level(sdk_log, DOCA_LOG_LEVEL_WARNING);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_log_backend_set_sdk_level failed", status);
		return EXIT_FAILURE;
	}

	printf("Requesting sflow_dedicated_output from Comch server '%s'...\n",
	       SFLOW_COMCH_SERVER_NAME);
	status = create_comch_client(argv[1], &state);
	if (status != DOCA_SUCCESS)
		goto cleanup;
	clock_gettime(CLOCK_MONOTONIC, &start_time);
	status = run_comch_client(&state);
	if (status != DOCA_SUCCESS) {
		log_doca_error("Comch request failed", status);
		goto cleanup;
	}
	if (validate_output(&state.output) != 0)
		goto cleanup;

	clock_gettime(CLOCK_MONOTONIC, &end_time);
	uint64_t elapsed_ns = (end_time.tv_sec - start_time.tv_sec) * 1000000000LL +
			     (end_time.tv_nsec - start_time.tv_nsec);
	printf("Comch request completed successfully in %" PRIu64 " ns\n", elapsed_ns);
	print_output(&state.output);
	exit_status = EXIT_SUCCESS;

cleanup:
	stop_and_destroy_client(&state);
	return exit_status;
}
