#!/usr/bin/env bash

set -euo pipefail

if [[ $(uname -m) != x86_64 ]]; then
	echo "error: run this check on the x86_64 host, not on BlueField Arm" >&2
	exit 1
fi

required_commands=(
	gcc
	ibv_devices
	meson
	ninja
	pkg-config
)

for command_name in "${required_commands[@]}"; do
	if ! command -v "$command_name" >/dev/null 2>&1; then
		echo "error: missing required command: $command_name" >&2
		exit 1
	fi
done

required_doca_tools=(
	/opt/mellanox/doca/tools/dpa-app-attributes2blob
	/opt/mellanox/doca/tools/dpacc
)

for tool_path in "${required_doca_tools[@]}"; do
	if [[ ! -x "$tool_path" ]]; then
		echo "error: missing $tool_path; install the matching DOCA-Host development profile" >&2
		exit 1
	fi
done

required_modules=(
	doca-common
	doca-dpa
	doca-verbs
	libibverbs
	libmlx5
	libflexio
)

for module_name in "${required_modules[@]}"; do
	if ! pkg-config --exists "$module_name"; then
		echo "error: pkg-config module is unavailable: $module_name" >&2
		exit 1
	fi
done

doca_library_triplet=x86_64-linux-gnu
if ! pkg-config --exists doca-flow &&
	[[ ! -e "/opt/mellanox/doca/tools/lib/${doca_library_triplet}/libdoca_flow.so" ]]; then
	echo "error: DOCA Flow development library is unavailable" >&2
	exit 1
fi

echo "x86 host build prerequisites: OK"
echo "DOCA DPA version: $(pkg-config --modversion doca-dpa)"
echo "Available RDMA devices:"
ibv_devices
