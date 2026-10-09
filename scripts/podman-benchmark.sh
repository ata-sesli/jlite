#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
output=${1:-"$root/benchmarks/results/fedora.json"}
image=localhost/jlite:v0-benchmark-tests
name="jlite-benchmark-$(date +%s)-$$"
volume="$name-data"
revision=$(git -C "$root" rev-parse HEAD)
if [ -n "$(git -C "$root" status --porcelain)" ]; then revision="$revision-dirty"; fi

mkdir -p "$(dirname -- "$output")"
echo "Building and verifying Go 1.27.1 benchmark image on the configured Podman engine..."
podman build --target build -f "$root/deploy/podman/Containerfile" -t "$image" "$root"

cleanup() {
  podman rm --force "$name" >/dev/null 2>&1 || true
  podman volume rm "$volume" >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM
podman volume create "$volume" >/dev/null
podman create --name "$name" --network=none --cpus=4 --memory=2g --pids-limit=512 \
  --volume "$volume:/bench-data" --env TMPDIR=/bench-data --env GOMAXPROCS=4 \
  --env JLITE_BENCH_OUTPUT=/tmp/results.json --env "JLITE_BENCH_ROUNDS=${JLITE_BENCH_ROUNDS:-3}" \
  --env "JLITE_BENCH_REVISION=$revision" \
  "$image" sh scripts/go.sh test -run '^TestOperatingBenchmark$' -count=1 -v -timeout=15m . >/dev/null
echo "Running isolated samples with four CPUs, 2 GiB memory and a dedicated data volume..."
if ! podman start --attach "$name"; then
  podman cp "$name:/tmp/results.json" "$output.partial" 2>/dev/null || true
  exit 1
fi
podman cp "$name:/tmp/results.json" "$output"
echo "Raw benchmark results: $output"
