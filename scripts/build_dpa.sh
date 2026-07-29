#!/usr/bin/env bash

set -euo pipefail

output_archive=$1
device_source=$2
attributes_yaml=$3
doca_lib_dir=$4
doca_include_dir=$5

build_dir=$(dirname "$output_archive")
attributes_blob="${build_dir}/sflow_dpa_attributes.blob"

mkdir -p "$build_dir"

/opt/mellanox/doca/tools/dpa-app-attributes2blob \
	"$attributes_yaml" \
	"$attributes_blob"

/opt/mellanox/doca/tools/dpacc \
	"$device_source" \
	--output-file "$output_archive" \
	--mcpu "nv-dpa-bf3,nv-dpa-cx7,nv-dpa-cx8,nv-dpa-cx9" \
	--hostcc gcc \
	--hostcc-options="-Werror -Wall -Wextra -Wno-deprecated-declarations -DFLEXIO_ALLOW_EXPERIMENTAL_API" \
	--devicecc-options="-Werror -Wall -Wextra -Wno-deprecated-declarations -O2 -DFLEXIO_DEV_ALLOW_EXPERIMENTAL_API" \
	--app-name sflow_dpa_app \
	--device-libs="-L${doca_lib_dir} -ldoca_dpa_dev -ldoca_dpa_dev_verbs -ldoca_dpa_dev_comm" \
	--flto \
	--common-include-path "$doca_include_dir" \
	--dpa-proc-attr "$attributes_blob"
