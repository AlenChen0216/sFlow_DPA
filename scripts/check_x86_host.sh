#!/usr/bin/env bash

set -euo pipefail

if [[ $(uname -m) != x86_64 ]]; then
	echo "error: run this check on the x86_64 host, not on BlueField Arm" >&2
	exit 1
fi

required_commands=(
	gcc
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

required_modules=(
	doca-common
	doca-comch
)

for module_name in "${required_modules[@]}"; do
	if ! pkg-config --exists "$module_name"; then
		echo "error: pkg-config module is unavailable: $module_name" >&2
		exit 1
	fi
done

echo "x86 host build prerequisites: OK"
echo "DOCA Comch version: $(pkg-config --modversion doca-comch)"
