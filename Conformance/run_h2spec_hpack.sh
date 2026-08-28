#!/usr/bin/env bash
set -euo pipefail

readonly h2spec_image="summerwind/h2spec@sha256:5f4a65c30cae8569558ced048b4bfe0dcf01a221e36767ae504ccd8348a7aeb0"
readonly requested_port="${H2SPEC_PORT:-9002}"
readonly script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly repository_root="$(cd -- "${script_dir}/.." && pwd)"
readonly server_log="$(mktemp "${TMPDIR:-/tmp}/http2-lean-hpack-smoke.XXXXXX.log")"

server_pid=""

cleanup() {
  if [[ -n "${server_pid}" ]]; then
    kill "${server_pid}" 2>/dev/null || true
    wait "${server_pid}" 2>/dev/null || true
  fi
  rm -f -- "${server_log}"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if ! [[ "${requested_port}" =~ ^[0-9]+$ ]]; then
  echo "H2SPEC_PORT must be an integer between 1 and 65535" >&2
  exit 2
fi
readonly port=$((10#${requested_port}))
if ((port < 1 || port > 65535)); then
  echo "H2SPEC_PORT must be an integer between 1 and 65535" >&2
  exit 2
fi

command -v bazel >/dev/null || {
  echo "bazel is required" >&2
  exit 127
}
command -v docker >/dev/null || {
  echo "docker is required" >&2
  exit 127
}

cd "${repository_root}"
bazel build //Conformance:hpack_smoke_server
if ! docker image inspect "${h2spec_image}" >/dev/null 2>&1; then
  docker pull --platform linux/amd64 "${h2spec_image}"
fi

readonly server_binary="$(bazel info bazel-bin)/Conformance/hpack_smoke_server"
"${server_binary}" "${port}" >"${server_log}" 2>&1 &
server_pid=$!

ready=false
for ((attempt = 0; attempt < 100; attempt++)); do
  if ! kill -0 "${server_pid}" 2>/dev/null; then
    cat "${server_log}" >&2
    echo "HPACK smoke server exited before becoming ready" >&2
    exit 1
  fi
  if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
    ready=true
    break
  fi
  sleep 0.05
done

if [[ "${ready}" != true ]]; then
  cat "${server_log}" >&2
  echo "HPACK smoke server did not become ready" >&2
  exit 1
fi

docker run --rm --platform linux/amd64 --network host "${h2spec_image}" \
  hpack generic/5 -h 127.0.0.1 -p "${port}"
