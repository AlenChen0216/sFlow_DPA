/*
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * DPU control plane and Comch server for the UDP/8888 DPA receive path.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <infiniband/verbs.h>

#include <doca_comch.h>
#include <doca_ctx.h>
#include <doca_dev.h>
#include <doca_dpa.h>
#include <doca_error.h>
#include <doca_flow.h>
#include <doca_log.h>
#include <doca_mmap.h>
#include <doca_pe.h>
#include <doca_types.h>
#include <doca_verbs.h>
#include <doca_verbs_bridge.h>

#include "../common/sflow_comch_protocol.h"
#include "../common/sflow_dpa_common.h"

/*
 * DPACC creates this object because scripts/build_dpa.sh uses
 * "--app-name sflow_dpa_app".
 */
extern struct doca_dpa_app *sflow_dpa_app;

/* DPACC also emits the host stub for the device RPC with this name. */
doca_dpa_func_t sflow_receive_rpc;

struct app_resources {
	struct doca_verbs_context *verbs_context;
	struct doca_verbs_pd *verbs_pd;
	struct doca_dev *dev;
	struct doca_dpa *dpa;
	bool dpa_started;

	struct doca_dpa_completion *completion;
	struct doca_verbs_eth_rq *eth_rq;
	doca_dpa_dev_completion_t completion_handle;
	doca_dpa_dev_verbs_eth_rq_t eth_rq_handle;

	bool flow_initialized;
	struct doca_flow_port *flow_port;
	struct doca_flow_pipe *udp_pipe;
	struct doca_flow_pipe_entry *udp_entry;

	struct sflow_host_memory *host_memory;
	struct ibv_mr *host_mr;
	struct doca_mmap *host_mmap;
	doca_dpa_dev_mmap_t host_mmap_handle;
	uint32_t receive_mkey;
};

struct comch_server_state {
	struct doca_pe *pe;
	struct doca_comch_server *server;
	struct doca_dev_rep *representor;
	struct doca_comch_connection *active_connection;

	const struct sflow_dedicated_output *output;
	const uint8_t *transfer_data;
	uint8_t *send_frame;
	uint32_t max_message_size;
	uint32_t transfer_length;
	uint32_t transfer_offset;
	uint32_t last_chunk_length;
	uint32_t request_id;
	uint32_t response_status;
	uint16_t response_type;

	bool transfer_active;
	bool context_started;
	bool context_idle;
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

/*
 * Find the requested PF and create a DOCA Verbs context for it.
 */
static doca_error_t create_verbs_context(const char *device_name,
					 struct doca_verbs_context **verbs_context)
{
	struct doca_devinfo **devinfo_list = NULL;
	uint32_t device_count = 0;
	doca_error_t status;
	uint32_t i;

	status = doca_devinfo_create_list(&devinfo_list, &device_count);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_devinfo_create_list failed", status);
		return status;
	}

	status = DOCA_ERROR_NOT_FOUND;
	for (i = 0; i < device_count; ++i) {
		char found_name[DOCA_DEVINFO_IBDEV_NAME_SIZE + 1] = {0};
		enum doca_pci_func_type function_type;

		if (doca_devinfo_get_ibdev_name(devinfo_list[i],
						found_name,
						DOCA_DEVINFO_IBDEV_NAME_SIZE) != DOCA_SUCCESS)
			continue;
		if (strcmp(found_name, device_name) != 0)
			continue;

		status = doca_dpa_cap_is_supported(devinfo_list[i]);
		if (status != DOCA_SUCCESS) {
			fprintf(stderr,
				"DPA is not supported through DOCA device '%s': %s\n",
				device_name,
				doca_error_get_descr(status));
			break;
		}

		status = doca_devinfo_get_pci_func_type(devinfo_list[i], &function_type);
		if (status != DOCA_SUCCESS) {
			log_doca_error("doca_devinfo_get_pci_func_type failed", status);
			break;
		}
		if (function_type != DOCA_PCI_FUNC_TYPE_PF) {
			fprintf(stderr, "%s is not a physical function (PF)\n", device_name);
			status = DOCA_ERROR_INVALID_VALUE;
			break;
		}

		status = doca_verbs_context_create(devinfo_list[i],
						   DOCA_VERBS_CONTEXT_CREATE_FLAGS_NONE,
						   verbs_context);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_verbs_context_create failed", status);
		break;
	}

	(void)doca_devinfo_destroy_list(devinfo_list);
	if (status == DOCA_ERROR_NOT_FOUND)
		fprintf(stderr, "No DOCA IB device named '%s' was found\n", device_name);

	return status;
}

static doca_error_t create_dpa(struct app_resources *resources)
{
	doca_error_t status;

	status = doca_verbs_pd_create(resources->verbs_context, &resources->verbs_pd);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_verbs_pd_create failed", status);
		return status;
	}

	status = doca_verbs_pd_as_doca_dev(resources->verbs_pd, &resources->dev);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_verbs_pd_as_doca_dev failed", status);
		return status;
	}

	status = doca_dpa_create(resources->dev, &resources->dpa);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_dpa_create failed", status);
		return status;
	}

	status = doca_dpa_set_app(resources->dpa, sflow_dpa_app);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_dpa_set_app failed", status);
		return status;
	}

	status = doca_dpa_start(resources->dpa);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_dpa_start failed", status);
		return status;
	}
	resources->dpa_started = true;

	return DOCA_SUCCESS;
}

static doca_error_t create_completion(struct app_resources *resources)
{
	doca_error_t status;

	status = doca_dpa_completion_create(resources->dpa,
					    SFLOW_RX_QUEUE_DEPTH,
					    &resources->completion);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_dpa_completion_create failed", status);
		return status;
	}

	status = doca_dpa_completion_start(resources->completion);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_dpa_completion_start failed", status);
		return status;
	}

	status = doca_dpa_completion_get_dpa_handle(resources->completion,
						    &resources->completion_handle);
	if (status != DOCA_SUCCESS)
		log_doca_error("doca_dpa_completion_get_dpa_handle failed", status);

	return status;
}

/*
 * Register the DPA-owned Ethernet receive queue. Queue memory and its DPA
 * completion are managed by DOCA; packet data lands in the MR registered
 * below.
 */
static doca_error_t create_receive_queue(struct app_resources *resources)
{
	struct doca_verbs_eth_rq_init_attr *attributes = NULL;
	doca_error_t status;
	doca_error_t destroy_status;

	status = doca_verbs_eth_rq_init_attr_create(&attributes);
	if (status != DOCA_SUCCESS)
		goto out;

	status = doca_verbs_eth_rq_init_attr_set_pd(attributes, resources->verbs_pd);
	if (status != DOCA_SUCCESS)
		goto out;
	status = doca_verbs_eth_rq_init_attr_set_wr_num(attributes,
							SFLOW_RX_QUEUE_DEPTH);
	if (status != DOCA_SUCCESS)
		goto out;
	status = doca_verbs_eth_rq_init_attr_set_max_sges(attributes, 1);
	if (status != DOCA_SUCCESS)
		goto out;
	status = doca_verbs_eth_rq_init_attr_set_queue_id(attributes,
							  SFLOW_RX_QUEUE_ID);
	if (status != DOCA_SUCCESS)
		goto out;
	status = doca_verbs_eth_rq_init_attr_set_dpa(attributes, resources->dpa);
	if (status != DOCA_SUCCESS)
		goto out;
	status = doca_verbs_eth_rq_init_attr_set_dpa_completion(attributes,
								resources->completion);
	if (status != DOCA_SUCCESS)
		goto out;
	status = doca_verbs_eth_rq_init_attr_set_ts_source_type(
		attributes,
		DOCA_VERBS_TS_SOURCE_DEFAULT);
	if (status != DOCA_SUCCESS)
		goto out;
	status = doca_verbs_eth_rq_init_attr_set_external_datapath_en(attributes, 1);
	if (status != DOCA_SUCCESS)
		goto out;

	status = doca_verbs_eth_rq_create(resources->verbs_context,
					  attributes,
					  &resources->eth_rq);
	if (status != DOCA_SUCCESS)
		goto out;

	status = doca_verbs_eth_rq_get_dpa_handle(resources->eth_rq,
						  resources->dpa,
						  &resources->eth_rq_handle);

out:
	if (status != DOCA_SUCCESS)
		log_doca_error("Failed to create the DPA Ethernet receive queue", status);
	if (attributes != NULL) {
		destroy_status = doca_verbs_eth_rq_init_attr_destroy(attributes);
		if (destroy_status != DOCA_SUCCESS)
			log_doca_error("doca_verbs_eth_rq_init_attr_destroy failed",
				       destroy_status);
	}
	return status;
}

/*
 * Allocate memory in the BlueField Arm process and register the same
 * allocation for receive DMA (ibverbs MR) and DPA access (DOCA mmap).
 */
static doca_error_t register_host_memory(struct app_resources *resources)
{
	struct ibv_pd *ibv_pd;
	void *allocation = NULL;
	doca_error_t status;
	int rc;

	rc = posix_memalign(&allocation, 4096, sizeof(*resources->host_memory));
	if (rc != 0) {
		fprintf(stderr, "posix_memalign failed: %s\n", strerror(rc));
		return DOCA_ERROR_NO_MEMORY;
	}
	memset(allocation, 0, sizeof(*resources->host_memory));
	resources->host_memory = allocation;

	ibv_pd = doca_verbs_bridge_verbs_pd_get_ibv_pd(resources->verbs_pd);
	if (ibv_pd == NULL) {
		fprintf(stderr, "doca_verbs_bridge_verbs_pd_get_ibv_pd returned NULL\n");
		return DOCA_ERROR_DRIVER;
	}

	resources->host_mr = ibv_reg_mr(
		ibv_pd,
		resources->host_memory,
		sizeof(*resources->host_memory),
		IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
	if (resources->host_mr == NULL) {
		fprintf(stderr, "ibv_reg_mr failed: %s\n", strerror(errno));
		return DOCA_ERROR_DRIVER;
	}
	/*
	 * DOCA's reference receive-on-DPA sample uses the MR rkey in the
	 * DPA-posted receive SGE.
	 */
	resources->receive_mkey = resources->host_mr->rkey;

	status = doca_mmap_create(&resources->host_mmap);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_mmap_set_permissions(resources->host_mmap,
					   DOCA_ACCESS_FLAG_LOCAL_READ_WRITE);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_mmap_set_memrange(resources->host_mmap,
					resources->host_memory,
					sizeof(*resources->host_memory));
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_mmap_add_dev(resources->host_mmap, resources->dev);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_mmap_start(resources->host_mmap);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_mmap_dev_get_dpa_handle(resources->host_mmap,
					      resources->dev,
					      &resources->host_mmap_handle);
	if (status != DOCA_SUCCESS)
		goto error;

	return DOCA_SUCCESS;

error:
	log_doca_error("Failed to register Arm memory as a DOCA mmap", status);
	return status;
}

/*
 * DOCA Flow must be initialized before an external data-path receive queue is
 * created. This is also the ordering used by NVIDIA's receive-on-DPA sample.
 */
static doca_error_t initialize_flow(struct app_resources *resources)
{
	struct doca_flow_cfg *config = NULL;
	doca_error_t status;
	doca_error_t destroy_status;

	status = doca_flow_cfg_create(&config);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_flow_cfg_create failed", status);
		goto out;
	}
	status = doca_flow_cfg_set_pipe_queues(config, 1);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_flow_cfg_set_pipe_queues failed", status);
		goto out;
	}
	status = doca_flow_cfg_set_mode_args(config, "vnf");
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_flow_cfg_set_mode_args failed", status);
		goto out;
	}
	status = doca_flow_init(config);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_flow_init failed", status);
		goto out;
	}
	resources->flow_initialized = true;

out:
	if (config != NULL) {
		destroy_status = doca_flow_cfg_destroy(config);
		if (destroy_status != DOCA_SUCCESS)
			log_doca_error("doca_flow_cfg_destroy failed", destroy_status);
	}
	return status;
}

static doca_error_t create_flow_port(struct app_resources *resources)
{
	struct doca_flow_port_cfg *port_config = NULL;
	doca_error_t status;
	doca_error_t destroy_status;

	status = doca_flow_port_cfg_create(&port_config);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_flow_port_cfg_create failed", status);
		goto out;
	}
	status = doca_flow_port_cfg_set_port_id(port_config, 0);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_flow_port_cfg_set_port_id failed", status);
		goto out;
	}
	status = doca_flow_port_cfg_set_dev(port_config, resources->dev);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_flow_port_cfg_set_dev failed", status);
		goto out;
	}
	status = doca_flow_port_start(port_config, &resources->flow_port);
	if (status != DOCA_SUCCESS)
		log_doca_error("doca_flow_port_start failed", status);

out:
	if (port_config != NULL) {
		destroy_status = doca_flow_port_cfg_destroy(port_config);
		if (destroy_status != DOCA_SUCCESS)
			log_doca_error("doca_flow_port_cfg_destroy failed", destroy_status);
	}
	return status;
}

/*
 * Register an exact hardware steering rule:
 *
 *     outer IPv4 + UDP + destination port 8888 -> DPA ETH RQ logical ID 0
 */
static doca_error_t register_udp_8888_flow(struct app_resources *resources)
{
	struct doca_flow_pipe_cfg *pipe_config = NULL;
	struct doca_flow_match entry_match = {0};
	struct doca_flow_match entry_match_mask = {0};
	struct doca_flow_fwd forwarding = {0};
	uint16_t queue_ids[] = {SFLOW_RX_QUEUE_ID};
	const char *failed_operation = NULL;
	doca_error_t status;
	doca_error_t destroy_status;

	entry_match.parser_meta.outer_l3_type = DOCA_FLOW_L3_META_IPV4;
	entry_match.parser_meta.outer_l4_type = DOCA_FLOW_L4_META_UDP;
	entry_match.outer.l3_type = DOCA_FLOW_L3_TYPE_IP4;
	entry_match.outer.l4_type_ext = DOCA_FLOW_L4_TYPE_EXT_UDP;
	entry_match.outer.udp.l4_port.dst_port =
		htons((uint16_t)SFLOW_UDP_DST_PORT);

	/*
	 * A control-pipe entry needs an explicit mask. Keep the outer L3/L4
	 * selector fields as protocol qualifiers (zero mask), and activate only
	 * the parser metadata and UDP destination port as match operands.
	 */
	entry_match_mask.parser_meta.outer_l3_type = UINT32_MAX;
	entry_match_mask.parser_meta.outer_l4_type = UINT32_MAX;
	entry_match_mask.outer.udp.l4_port.dst_port = UINT16_MAX;

	forwarding.type = DOCA_FLOW_FWD_RSS;
	forwarding.rss_type = DOCA_FLOW_RESOURCE_TYPE_NON_SHARED;
	forwarding.rss.queues_array = queue_ids;
	forwarding.rss.nr_queues = 1;
	forwarding.rss.outer_flags = DOCA_FLOW_RSS_IPV4 | DOCA_FLOW_RSS_UDP;

	status = doca_flow_pipe_cfg_create(&pipe_config, resources->flow_port);
	if (status != DOCA_SUCCESS) {
		failed_operation = "doca_flow_pipe_cfg_create failed";
		goto out;
	}
	status = doca_flow_pipe_cfg_set_name(pipe_config, "UDP_8888_TO_DPA");
	if (status != DOCA_SUCCESS) {
		failed_operation = "doca_flow_pipe_cfg_set_name failed";
		goto out;
	}
	status = doca_flow_pipe_cfg_set_type(pipe_config, DOCA_FLOW_PIPE_CONTROL);
	if (status != DOCA_SUCCESS) {
		failed_operation = "doca_flow_pipe_cfg_set_type failed";
		goto out;
	}
	status = doca_flow_pipe_cfg_set_is_root(pipe_config, true);
	if (status != DOCA_SUCCESS) {
		failed_operation = "doca_flow_pipe_cfg_set_is_root failed";
		goto out;
	}
	status = doca_flow_pipe_create(pipe_config, NULL, NULL, &resources->udp_pipe);
	if (status != DOCA_SUCCESS) {
		failed_operation = "doca_flow_pipe_create failed";
		goto out;
	}

	status = doca_flow_pipe_control_add_entry(0,
						  resources->udp_pipe,
						  &entry_match,
						  &entry_match_mask,
						  NULL,
						  NULL,
						  NULL,
						  NULL,
						  NULL,
						  0,
						  &forwarding,
						  NULL,
						  &resources->udp_entry);
	if (status != DOCA_SUCCESS) {
		failed_operation = "doca_flow_pipe_control_add_entry failed";
		goto out;
	}

	status = doca_flow_entries_process(resources->flow_port, 0, 10000, 1);
	if (status != DOCA_SUCCESS) {
		failed_operation = "doca_flow_entries_process failed";
		goto out;
	}
	if (doca_flow_pipe_entry_get_status(resources->udp_entry) !=
	    DOCA_FLOW_ENTRY_STATUS_SUCCESS) {
		fprintf(stderr, "UDP/8888 flow entry was rejected during hardware offload\n");
		status = DOCA_ERROR_DRIVER;
	}

out:
	if (status != DOCA_SUCCESS && failed_operation != NULL)
		log_doca_error(failed_operation, status);
	if (pipe_config != NULL) {
		destroy_status = doca_flow_pipe_cfg_destroy(pipe_config);
		if (destroy_status != DOCA_SUCCESS)
			log_doca_error("doca_flow_pipe_cfg_destroy failed", destroy_status);
	}
	return status;
}

static doca_error_t create_resources(const char *device_name,
				     struct app_resources *resources)
{
	doca_error_t status;

	status = initialize_flow(resources);
	if (status != DOCA_SUCCESS)
		return status;
	status = create_verbs_context(device_name, &resources->verbs_context);
	if (status != DOCA_SUCCESS)
		return status;
	status = create_dpa(resources);
	if (status != DOCA_SUCCESS)
		return status;
	status = create_completion(resources);
	if (status != DOCA_SUCCESS)
		return status;
	status = create_receive_queue(resources);
	if (status != DOCA_SUCCESS)
		return status;
	status = create_flow_port(resources);
	if (status != DOCA_SUCCESS)
		return status;
	status = register_udp_8888_flow(resources);
	if (status != DOCA_SUCCESS)
		return status;
	return register_host_memory(resources);
}

static void destroy_resources(struct app_resources *resources)
{
	doca_error_t status;

	if (resources->udp_pipe != NULL)
		doca_flow_pipe_destroy(resources->udp_pipe);
	if (resources->flow_port != NULL) {
		status = doca_flow_port_stop(resources->flow_port);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_flow_port_stop failed", status);
	}
	if (resources->host_mmap != NULL) {
		status = doca_mmap_destroy(resources->host_mmap);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_mmap_destroy failed", status);
	}
	if (resources->host_mr != NULL && ibv_dereg_mr(resources->host_mr) != 0)
		fprintf(stderr, "ibv_dereg_mr failed: %s\n", strerror(errno));
	free(resources->host_memory);

	if (resources->eth_rq != NULL) {
		status = doca_verbs_eth_rq_destroy(resources->eth_rq);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_verbs_eth_rq_destroy failed", status);
	}
	if (resources->completion != NULL) {
		status = doca_dpa_completion_destroy(resources->completion);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_dpa_completion_destroy failed", status);
	}
	if (resources->dpa_started) {
		status = doca_dpa_stop(resources->dpa);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_dpa_stop failed", status);
	}
	if (resources->dpa != NULL) {
		status = doca_dpa_destroy(resources->dpa);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_dpa_destroy failed", status);
	}
	if (resources->dev != NULL) {
		status = doca_dev_close(resources->dev);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_dev_close failed", status);
	}
	if (resources->verbs_pd != NULL) {
		status = doca_verbs_pd_destroy(resources->verbs_pd);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_verbs_pd_destroy failed", status);
	}
	if (resources->verbs_context != NULL) {
		status = doca_verbs_context_destroy(resources->verbs_context);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_verbs_context_destroy failed", status);
	}
	if (resources->flow_initialized)
		doca_flow_destroy();
}

static int parse_packet_count(const char *value, uint32_t *packet_count)
{
	char *end = NULL;
	unsigned long parsed;

	errno = 0;
	parsed = strtoul(value, &end, 10);
	if (errno != 0 || end == value || *end != '\0' ||
	    parsed == 0 || parsed > SFLOW_RX_QUEUE_DEPTH)
		return -1;

	*packet_count = (uint32_t)parsed;
	return 0;
}

/*
 * doca_dpa_rpc() is blocking. The DPA kernel writes this Arm-process-owned
 * allocation, executes __dpa_thread_window_writeback(), and only then returns.
 * Copying the record gives Comch a stable snapshot for all client requests.
 */
static int snapshot_output(const struct sflow_dedicated_output *shared_output,
			   uint32_t expected_packets,
			   struct sflow_dedicated_output *snapshot)
{
	memcpy(snapshot, shared_output, sizeof(*snapshot));

	if (snapshot->abi_version != SFLOW_OUTPUT_ABI_VERSION) {
		fprintf(stderr,
			"Invalid DPA output ABI version: got %" PRIu32 ", expected %u\n",
			snapshot->abi_version,
			SFLOW_OUTPUT_ABI_VERSION);
		return -1;
	}
	if (snapshot->packets_received != expected_packets) {
		fprintf(stderr,
			"Incomplete DPA output: got %" PRIu64
			" packet(s), expected %" PRIu32 "\n",
			snapshot->packets_received,
			expected_packets);
		return -1;
	}
	if (snapshot->last_packet_length > SFLOW_MAX_PACKET_SIZE) {
		fprintf(stderr,
			"Invalid last packet length in DPA output: %" PRIu32 "\n",
			snapshot->last_packet_length);
		return -1;
	}

	return 0;
}

static doca_error_t open_representor(struct doca_dev *dev,
				    const char *pci_address,
				    struct doca_dev_rep **representor)
{
	struct doca_devinfo_rep **representor_list = NULL;
	uint32_t representor_count = 0;
	doca_error_t status;
	uint32_t i;

	*representor = NULL;
	status = doca_devinfo_rep_create_list(dev,
					     DOCA_DEVINFO_REP_FILTER_NET,
					     &representor_list,
					     &representor_count);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_devinfo_rep_create_list failed", status);
		return status;
	}

	status = DOCA_ERROR_NOT_FOUND;
	for (i = 0; i < representor_count; ++i) {
		uint8_t address_matches = 0;

		if (doca_devinfo_rep_is_equal_pci_addr(representor_list[i],
						      pci_address,
						      &address_matches) != DOCA_SUCCESS ||
		    address_matches == 0)
			continue;

		status = doca_dev_rep_open(representor_list[i], representor);
		break;
	}

	(void)doca_devinfo_rep_destroy_list(representor_list);
	if (status != DOCA_SUCCESS) {
		fprintf(stderr,
			"Unable to open net representor '%s': %s\n",
			pci_address,
			doca_error_get_descr(status));
	}
	return status;
}

static void encode_header(struct sflow_comch_header *header,
			  uint16_t message_type,
			  uint32_t request_id,
			  uint32_t status,
			  uint32_t output_abi_version,
			  uint32_t total_length,
			  uint32_t offset,
			  uint32_t payload_length)
{
	header->magic = htonl(SFLOW_COMCH_PROTOCOL_MAGIC);
	header->protocol_version = htons(SFLOW_COMCH_PROTOCOL_VERSION);
	header->message_type = htons(message_type);
	header->request_id = htonl(request_id);
	header->status = htonl(status);
	header->output_abi_version = htonl(output_abi_version);
	header->total_length = htonl(total_length);
	header->offset = htonl(offset);
	header->payload_length = htonl(payload_length);
}

static doca_error_t submit_next_response_chunk(struct comch_server_state *state)
{
	struct sflow_comch_header *header =
		(struct sflow_comch_header *)state->send_frame;
	struct doca_comch_task_send *send_task = NULL;
	struct doca_task *task;
	union doca_data task_data = {0};
	uint32_t remaining;
	uint32_t payload_capacity;
	uint32_t payload_length;
	doca_error_t status;

	if (!state->transfer_active || state->active_connection == NULL)
		return DOCA_ERROR_BAD_STATE;

	payload_capacity = state->max_message_size - sizeof(*header);
	remaining = state->transfer_length - state->transfer_offset;
	payload_length = remaining < payload_capacity ? remaining : payload_capacity;

	encode_header(header,
		      state->response_type,
		      state->request_id,
		      state->response_status,
		      state->response_type == SFLOW_COMCH_MSG_OUTPUT_CHUNK ?
			      SFLOW_OUTPUT_ABI_VERSION :
			      0,
		      state->transfer_length,
		      state->transfer_offset,
		      payload_length);
	if (payload_length != 0) {
		memcpy(state->send_frame + sizeof(*header),
		       state->transfer_data + state->transfer_offset,
		       payload_length);
	}
	state->last_chunk_length = payload_length;

	status = doca_comch_server_task_send_alloc_init(
		state->server,
		state->active_connection,
		state->send_frame,
		sizeof(*header) + payload_length,
		&send_task);
	if (status != DOCA_SUCCESS)
		return status;

	task = doca_comch_task_send_as_task(send_task);
	task_data.ptr = state;
	doca_task_set_user_data(task, task_data);
	status = doca_task_submit(task);
	if (status != DOCA_SUCCESS)
		doca_task_free(task);
	return status;
}

static void finish_transfer(struct comch_server_state *state)
{
	state->transfer_active = false;
	state->active_connection = NULL;
	state->transfer_data = NULL;
	state->transfer_length = 0;
	state->transfer_offset = 0;
	state->last_chunk_length = 0;
}

static void send_task_completion_callback(struct doca_comch_task_send *send_task,
					  union doca_data task_user_data,
					  union doca_data context_user_data)
{
	struct comch_server_state *state = context_user_data.ptr;
	doca_error_t status;

	(void)task_user_data;
	doca_task_free(doca_comch_task_send_as_task(send_task));

	state->transfer_offset += state->last_chunk_length;
	if (state->transfer_offset >= state->transfer_length) {
		if (state->response_type == SFLOW_COMCH_MSG_OUTPUT_CHUNK) {
			printf("Served sflow_dedicated_output request %" PRIu32
			       " (%" PRIu32 " bytes)\n",
			       state->request_id,
			       state->transfer_length);
		}
		finish_transfer(state);
		return;
	}

	status = submit_next_response_chunk(state);
	if (status != DOCA_SUCCESS) {
		log_doca_error("Failed to submit next Comch response chunk", status);
		finish_transfer(state);
	}
}

static void send_task_error_callback(struct doca_comch_task_send *send_task,
				     union doca_data task_user_data,
				     union doca_data context_user_data)
{
	struct comch_server_state *state = context_user_data.ptr;
	doca_error_t status =
		doca_task_get_status(doca_comch_task_send_as_task(send_task));

	(void)task_user_data;
	log_doca_error("Comch response send failed", status);
	doca_task_free(doca_comch_task_send_as_task(send_task));
	finish_transfer(state);
}

static doca_error_t begin_response(struct comch_server_state *state,
				   struct doca_comch_connection *connection,
				   uint32_t request_id,
				   uint16_t response_type,
				   uint32_t response_status,
				   const void *data,
				   uint32_t data_length)
{
	state->active_connection = connection;
	state->request_id = request_id;
	state->response_type = response_type;
	state->response_status = response_status;
	state->transfer_data = data;
	state->transfer_length = data_length;
	state->transfer_offset = 0;
	state->last_chunk_length = 0;
	state->transfer_active = true;

	return submit_next_response_chunk(state);
}

static void message_receive_callback(struct doca_comch_event_msg_recv *event,
				     uint8_t *receive_buffer,
				     uint32_t message_length,
				     struct doca_comch_connection *connection)
{
	static const char malformed_request[] = "invalid GET_OUTPUT request";
	struct doca_comch_server *server =
		doca_comch_server_get_server_ctx(connection);
	struct sflow_comch_header request;
	struct comch_server_state *state;
	union doca_data context_data = {0};
	uint32_t request_id = 0;
	doca_error_t status;
	bool valid_request;

	(void)event;
	status = doca_ctx_get_user_data(doca_comch_server_as_ctx(server),
					&context_data);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_ctx_get_user_data failed", status);
		return;
	}
	state = context_data.ptr;

	valid_request = message_length == sizeof(request);
	if (valid_request) {
		memcpy(&request, receive_buffer, sizeof(request));
		request_id = ntohl(request.request_id);
		valid_request =
			ntohl(request.magic) == SFLOW_COMCH_PROTOCOL_MAGIC &&
			ntohs(request.protocol_version) ==
				SFLOW_COMCH_PROTOCOL_VERSION &&
			ntohs(request.message_type) ==
				SFLOW_COMCH_MSG_GET_OUTPUT &&
			ntohl(request.status) == SFLOW_COMCH_STATUS_OK &&
			ntohl(request.output_abi_version) == 0 &&
			ntohl(request.total_length) == 0 &&
			ntohl(request.offset) == 0 &&
			ntohl(request.payload_length) == 0;
	}

	if (state->transfer_active) {
		fprintf(stderr,
			"Ignoring Comch request while response %" PRIu32
			" is still in progress\n",
			state->request_id);
		return;
	}

	if (!valid_request) {
		status = begin_response(state,
					connection,
					request_id,
					SFLOW_COMCH_MSG_ERROR,
					SFLOW_COMCH_STATUS_BAD_REQUEST,
					malformed_request,
					sizeof(malformed_request) - 1);
	} else {
		status = begin_response(state,
					connection,
					request_id,
					SFLOW_COMCH_MSG_OUTPUT_CHUNK,
					SFLOW_COMCH_STATUS_OK,
					state->output,
					sizeof(*state->output));
	}
	if (status != DOCA_SUCCESS) {
		log_doca_error("Failed to submit Comch response", status);
		finish_transfer(state);
	}
}

static void connection_callback(
	struct doca_comch_event_connection_status_changed *event,
	struct doca_comch_connection *connection,
	uint8_t change_succeeded)
{
	(void)event;
	(void)connection;
	if (change_succeeded != 0)
		printf("Comch client connected\n");
	else
		fprintf(stderr, "Comch client connection failed\n");
}

static void disconnection_callback(
	struct doca_comch_event_connection_status_changed *event,
	struct doca_comch_connection *connection,
	uint8_t change_succeeded)
{
	(void)event;
	(void)connection;
	(void)change_succeeded;
	printf("Comch client disconnected\n");
}

static void context_state_changed_callback(const union doca_data user_data,
					   struct doca_ctx *context,
					   enum doca_ctx_states previous_state,
					   enum doca_ctx_states next_state)
{
	struct comch_server_state *state = user_data.ptr;

	(void)context;
	(void)previous_state;
	if (next_state == DOCA_CTX_STATE_IDLE)
		state->context_idle = true;
}

static doca_error_t create_comch_server(struct doca_dev *dev,
					const char *representor_pci_address,
					const struct sflow_dedicated_output *output,
					struct comch_server_state *state)
{
	struct doca_ctx *context;
	union doca_data context_data = {0};
	doca_error_t status;

	state->output = output;

	status = doca_comch_cap_server_is_supported(doca_dev_as_devinfo(dev));
	if (status != DOCA_SUCCESS) {
		log_doca_error("DOCA device does not support a Comch server", status);
		return status;
	}
	status = open_representor(dev,
				  representor_pci_address,
				  &state->representor);
	if (status != DOCA_SUCCESS)
		return status;
	status = doca_comch_cap_get_max_msg_size(doca_dev_as_devinfo(dev),
						 &state->max_message_size);
	if (status != DOCA_SUCCESS)
		goto error;
	if (state->max_message_size <= sizeof(struct sflow_comch_header)) {
		fprintf(stderr,
			"Comch maximum message size (%" PRIu32
			") is too small for the protocol header\n",
			state->max_message_size);
		status = DOCA_ERROR_NOT_SUPPORTED;
		goto error;
	}
	state->send_frame = malloc(state->max_message_size);
	if (state->send_frame == NULL) {
		status = DOCA_ERROR_NO_MEMORY;
		goto error;
	}

	status = doca_pe_create(&state->pe);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_server_create(dev,
					  state->representor,
					  SFLOW_COMCH_SERVER_NAME,
					  &state->server);
	if (status != DOCA_SUCCESS)
		goto error;
	context = doca_comch_server_as_ctx(state->server);

	status = doca_pe_connect_ctx(state->pe, context);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_ctx_set_state_changed_cb(context,
					       context_state_changed_callback);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_server_task_send_set_conf(
		state->server,
		send_task_completion_callback,
		send_task_error_callback,
		SFLOW_COMCH_SEND_TASK_COUNT);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_server_event_msg_recv_register(
		state->server,
		message_receive_callback);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_server_event_connection_status_changed_register(
		state->server,
		connection_callback,
		disconnection_callback);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_server_set_max_msg_size(state->server,
						    state->max_message_size);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_comch_server_set_recv_queue_size(
		state->server,
		SFLOW_COMCH_RECV_QUEUE_SIZE);
	if (status != DOCA_SUCCESS)
		goto error;

	context_data.ptr = state;
	status = doca_ctx_set_user_data(context, context_data);
	if (status != DOCA_SUCCESS)
		goto error;
	status = doca_ctx_start(context);
	if (status != DOCA_SUCCESS)
		goto error;
	state->context_started = true;

	return DOCA_SUCCESS;

error:
	log_doca_error("Failed to create Comch server", status);
	return status;
}

static void stop_and_destroy_comch_server(struct comch_server_state *state)
{
	struct doca_ctx *context;
	doca_error_t status;

	if (state->server != NULL) {
		context = doca_comch_server_as_ctx(state->server);
		if (state->context_started && !state->context_idle) {
			status = doca_ctx_stop(context);
			if (status != DOCA_SUCCESS &&
			    status != DOCA_ERROR_IN_PROGRESS)
				log_doca_error("doca_ctx_stop(Comch server) failed",
					       status);
			while (!state->context_idle)
				(void)doca_pe_progress(state->pe);
		}
		status = doca_comch_server_destroy(state->server);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_comch_server_destroy failed", status);
	}
	if (state->pe != NULL) {
		status = doca_pe_destroy(state->pe);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_pe_destroy failed", status);
	}
	if (state->representor != NULL) {
		status = doca_dev_rep_close(state->representor);
		if (status != DOCA_SUCCESS)
			log_doca_error("doca_dev_rep_close failed", status);
	}
	free(state->send_frame);
}

static doca_error_t run_comch_server(struct comch_server_state *state)
{
	const struct timespec idle_delay = {
		.tv_sec = 0,
		.tv_nsec = 10000,
	};
	struct doca_ctx *context = doca_comch_server_as_ctx(state->server);
	doca_error_t status;

	printf("Comch server '%s' is ready; press Ctrl-C to stop\n",
	       SFLOW_COMCH_SERVER_NAME);
	while (!stop_requested && !state->context_idle) {
		if (doca_pe_progress(state->pe) == 0)
			(void)nanosleep(&idle_delay, NULL);
	}

	if (!state->context_idle) {
		status = doca_ctx_stop(context);
		if (status != DOCA_SUCCESS && status != DOCA_ERROR_IN_PROGRESS) {
			log_doca_error("doca_ctx_stop(Comch server) failed", status);
			return status;
		}
		while (!state->context_idle)
			(void)doca_pe_progress(state->pe);
	}

	return DOCA_SUCCESS;
}

int main(int argc, char **argv)
{
	struct app_resources resources = {0};
	struct comch_server_state comch_state = {0};
	struct sflow_dedicated_output output_snapshot;
	struct doca_log_backend *sdk_log = NULL;
	uint32_t packet_count = 1;
	uint64_t rpc_return_value = UINT64_MAX;
	doca_error_t status;
	int exit_status = EXIT_FAILURE;

	if (argc < 3 || argc > 4) {
		fprintf(stderr,
			"Usage: %s <mlx5-device> <host-PF-representor-pci> "
			"[packet-count: 1-%u]\n",
			argv[0],
			SFLOW_RX_QUEUE_DEPTH);
		return EXIT_FAILURE;
	}
	if (argc == 4 && parse_packet_count(argv[3], &packet_count) != 0) {
		fprintf(stderr, "Invalid packet count '%s'; expected 1-%u\n",
			argv[3],
			SFLOW_RX_QUEUE_DEPTH);
		return EXIT_FAILURE;
	}
	if (geteuid() != 0) {
		fprintf(stderr, "This program must run with root privileges\n");
		return EXIT_FAILURE;
	}

#if !defined(__aarch64__)
	fprintf(stderr,
		"This Comch server must be built and run on the BlueField Arm cores\n");
	return EXIT_FAILURE;
#endif

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

	status = create_resources(argv[1], &resources);
	if (status != DOCA_SUCCESS)
		goto cleanup;

	printf("Registered %zu bytes of BlueField Arm memory at %p\n",
	       sizeof(*resources.host_memory),
	       (void *)resources.host_memory);
	printf("Waiting for %" PRIu32 " IPv4/UDP packet(s) to destination port %u...\n",
	       packet_count,
	       SFLOW_UDP_DST_PORT);
	fflush(stdout);

	status = doca_dpa_rpc(resources.dpa,
			      &sflow_receive_rpc,
			      &rpc_return_value,
			      resources.eth_rq_handle,
			      resources.completion_handle,
			      resources.host_mmap_handle,
			      (uint64_t)(uintptr_t)resources.host_memory,
			      resources.receive_mkey,
			      packet_count);
	if (status != DOCA_SUCCESS) {
		log_doca_error("doca_dpa_rpc failed", status);
		goto cleanup;
	}
	if (rpc_return_value != 0) {
		fprintf(stderr, "DPA receive RPC returned error code %" PRIu64 "\n",
			rpc_return_value);
		goto cleanup;
	}

	if (snapshot_output(&resources.host_memory->output,
			    packet_count,
			    &output_snapshot) != 0)
		goto cleanup;

	printf("Captured sflow_dedicated_output: %" PRIu64
	       " packet(s), %zu bytes\n",
	       output_snapshot.packets_received,
	       sizeof(output_snapshot));

	if (signal(SIGINT, handle_stop_signal) == SIG_ERR ||
	    signal(SIGTERM, handle_stop_signal) == SIG_ERR) {
		fprintf(stderr, "Failed to install signal handlers: %s\n",
			strerror(errno));
		goto cleanup;
	}
	status = create_comch_server(resources.dev,
				     argv[2],
				     &output_snapshot,
				     &comch_state);
	if (status != DOCA_SUCCESS)
		goto cleanup;
	status = run_comch_server(&comch_state);
	if (status != DOCA_SUCCESS)
		goto cleanup;

	exit_status = EXIT_SUCCESS;

cleanup:
	stop_and_destroy_comch_server(&comch_state);
	destroy_resources(&resources);
	return exit_status;
}
