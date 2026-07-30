project(
	'sflow_dpa',
	'c',
	version: '0.1.0',
	default_options: ['buildtype=debugoptimized', 'c_std=c11', 'warning_level=3'],
	meson_version: '>=0.61.2',
)

add_project_arguments(
	'-D_GNU_SOURCE',
	'-DDOCA_ALLOW_EXPERIMENTAL_API',
	language: 'c',
)

cc = meson.get_compiler('c')

doca_common_dep = dependency('doca-common')
doca_comch_dep = dependency('doca-comch')

if host_machine.cpu_family() == 'aarch64'
	doca_dpa_dep = dependency('doca-dpa')
	doca_verbs_dep = dependency('doca-verbs')
	ibverbs_dep = dependency('libibverbs')
	mlx5_dep = dependency('libmlx5')
	flexio_dep = dependency('libflexio')

	# Some BlueField development images keep DOCA Flow's development files
	# under /opt/mellanox/doca/tools instead of the normal SDK prefix.
	doca_flow_dep = dependency('doca-flow', required: false)
	extra_include_dirs = []
	extra_build_rpath = ''
	if not doca_flow_dep.found()
		doca_tools_include = '/opt/mellanox/doca/tools/include'
		doca_tools_lib = '/opt/mellanox/doca/tools/lib/aarch64-linux-gnu'
		extra_include_dirs += include_directories(doca_tools_include)
		doca_flow_dep = cc.find_library('doca_flow', dirs: doca_tools_lib)
		extra_build_rpath = doca_tools_lib
	endif

	dpa_lib_dir = doca_dpa_dep.get_variable(pkgconfig: 'libdir')
	dpa_include_dir = doca_dpa_dep.get_variable(pkgconfig: 'includedir')
	build_dpa = find_program('scripts/build_dpa.sh')
	dpa_program = custom_target(
		'sflow_dpa_program',
		input: [
			'dev/kernel.c',
			'dev/attributes.yaml',
			'common/sflow_dpa_common.h',
		],
		output: 'libsflow_dpa_program.a',
		command: [
			build_dpa,
			'@OUTPUT@',
			'@INPUT0@',
			'@INPUT1@',
			dpa_lib_dir,
			dpa_include_dir,
		],
		build_by_default: true,
	)
	dpa_program_dep = declare_dependency(
		link_with: dpa_program,
	)

	executable(
		'sflow_dpa_comch_server',
		'dpu/sflow_dpa_comch_server.c',
		dependencies: [
			doca_common_dep,
			doca_comch_dep,
			doca_dpa_dep,
			doca_verbs_dep,
			doca_flow_dep,
			ibverbs_dep,
			mlx5_dep,
			flexio_dep,
			dpa_program_dep,
		],
		include_directories: extra_include_dirs,
		build_rpath: extra_build_rpath,
		install: false,
	)
	executable(
		'sflow_dpa_comch_server_producer_consumer',
		'dpu/sflow_dpa_comch_server_producer_consumer.c',
		dependencies: [
			doca_common_dep,
			doca_comch_dep,
			doca_dpa_dep,
			doca_verbs_dep,
			doca_flow_dep,
			ibverbs_dep,
			mlx5_dep,
			flexio_dep,
			dpa_program_dep,
		],
		include_directories: extra_include_dirs,
		build_rpath: extra_build_rpath,
		install: false,
	)
elif host_machine.cpu_family() == 'x86_64'
	executable(
		'sflow_dpa_comch_client',
		'host/sflow_dpa_comch_client.c',
		dependencies: [
			doca_common_dep,
			doca_comch_dep,
		],
		install: false,
	)
	executable(
		'sflow_dpa_comch_client_producer_consumer',
		'host/sflow_dpa_comch_client_producer_consumer.c',
		dependencies: [
			doca_common_dep,
			doca_comch_dep,
		],
		install: false,
	)
else
	error('Only BlueField aarch64 and x86_64 builds are supported')
endif
