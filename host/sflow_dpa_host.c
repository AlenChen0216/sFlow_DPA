/*
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Host control plane for the UDP/8888 DPA receive skeleton.
 */

#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <infiniband/verbs.h>

#include <doca_dev.h>
#include <doca_dpa.h>
#include <doca_error.h>
#include <doca_flow.h>
#include <doca_log.h>
#include <doca_mmap.h>
#include <doca_types.h>
#include <doca_verbs.h>
#include <doca_verbs_bridge.h>

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

static void log_doca_error(const char *operation, doca_error_t status)
{
	fprintf(stderr, "%s: %s\n", operation, doca_error_get_descr(status));
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
 * Allocate memory in the host process and register the exact same allocation
 * for both receive DMA (ibverbs MR) and direct DPA access (DOCA mmap).
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
	log_doca_error("Failed to register host memory as a DOCA mmap", status);
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

static void print_output(const struct sflow_dedicated_output *output)
{
	uint32_t length = output->dedicated_data_length;
	uint32_t i;

	if (length > SFLOW_DEDICATED_DATA_CAPACITY)
		length = SFLOW_DEDICATED_DATA_CAPACITY;

	printf("DPA output:\n");
	printf("  ABI version:          %" PRIu32 "\n", output->abi_version);
	printf("  packets received:     %" PRIu64 "\n", output->packets_received);
	printf("  last packet length:   %" PRIu32 "\n", output->last_packet_length);
	printf("  last packet timestamp:%" PRIu64 "\n", output->last_packet_timestamp);
	printf("  dedicated data bytes: %" PRIu32 "\n", length);

	if (length == 0) {
		printf("  dedicated data:       <empty; implement store_dedicated_data()>\n");
		return;
	}

	printf("  dedicated data:");
	for (i = 0; i < SFLOW_DEDICATED_DATA_CAPACITY; ++i) {
		if (output->dedicated_data[i].data.cnt > 0) {
			printf("\n    key: %s, count: %" PRIu32,
			       output->dedicated_data[i].key,
			       output->dedicated_data[i].data.cnt);
		}
	}
	printf("\n");
}

int main(int argc, char **argv)
{
	struct app_resources resources = {0};
	struct doca_log_backend *sdk_log = NULL;
	uint32_t packet_count = 1;
	uint64_t rpc_return_value = UINT64_MAX;
	doca_error_t status;
	int exit_status = EXIT_FAILURE;

	if (argc < 2 || argc > 3) {
		fprintf(stderr, "Usage: %s <mlx5-device> [packet-count: 1-%u]\n",
			argv[0],
			SFLOW_RX_QUEUE_DEPTH);
		return EXIT_FAILURE;
	}
	if (argc == 3 && parse_packet_count(argv[2], &packet_count) != 0) {
		fprintf(stderr, "Invalid packet count '%s'; expected 1-%u\n",
			argv[2],
			SFLOW_RX_QUEUE_DEPTH);
		return EXIT_FAILURE;
	}
	if (geteuid() != 0) {
		fprintf(stderr, "This program must run with root privileges\n");
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

	status = create_resources(argv[1], &resources);
	if (status != DOCA_SUCCESS)
		goto cleanup;

	printf("Registered %zu bytes of host memory at %p\n",
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

	print_output(&resources.host_memory->output);
	exit_status = EXIT_SUCCESS;

cleanup:
	destroy_resources(&resources);
	return exit_status;
}
