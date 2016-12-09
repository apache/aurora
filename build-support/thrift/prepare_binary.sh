#!/usr/bin/env bash

set -o xtrace
set -o errexit
set -o nounset

readonly HERE=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)

readonly SERVE_ROOT=${HERE}/serve

# TODO(John Sirois): Eliminate this list (which needs to be expanded each time OSX has a new
# release) when pants supports querying the current os id or else a psuedo-id like `local`.
readonly KNOWN_OS_IDS=(
  linux/i386
  linux/x86_64
  mac/10.6
  mac/10.7
  mac/10.8
  mac/10.9
  mac/10.10
  mac/10.11
  mac/10.12
)

# Runs pants safely as an indirect pants subprocess.
function run_pants() {
  (
    cd "${HERE}/../.."
    ./pants --no-colors --no-lock "$@"
  )
}

# Returns the pants option value for the given scope and name.
function get_pants_option() {
  readonly scope=$1
  readonly name=$2

  run_pants options --scope=${scope} --name=${name} --output-format=json 2>/dev/null | \
    python2.7 -c "import json, sys; print(json.load(sys.stdin)['${scope}.${name}']['value'])"
}

# Ensures the thrift binary of the given version is built for the current machine and returns its
# path.
function bootstrap_thrift() {
  readonly version=$1

  # This bootstraps the thrift compiler via `make` if needed.
  ${HERE}/thriftw ${version} --version >&2 || true

  # This reports the path of the actual thrift binary used.
  ${HERE}/thriftw ${version} --which
}

# Sets up the thrift binary serving directory structure under SERVE_ROOT.
function prepare_serve() {
  rm -rf ${SERVE_ROOT}

  readonly thrift_version=$(get_pants_option thrift-binary version)
  readonly thrift_binary=$(bootstrap_thrift ${thrift_version})

  readonly thrift_bin_dir=$(get_pants_option thrift-binary supportdir)
  for os_id in "${KNOWN_OS_IDS[@]}"
  do
    serve_dir=${SERVE_ROOT}/${thrift_bin_dir}/${os_id}/${thrift_version}

    mkdir -p ${serve_dir}
    ln -f -s ${thrift_binary} ${serve_dir}/thrift
  done
}

prepare_serve

