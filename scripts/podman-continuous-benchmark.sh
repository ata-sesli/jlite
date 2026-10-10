#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
output=${1:-"$root/benchmarks/results/continuous.json"}
image=localhost/jlite:continuous-benchmark-tests
name="jlite-continuous-$(date +%s)-$$"
volume="$name-data"
revision=$(git -C "$root" rev-parse HEAD)
if [ -n "$(git -C "$root" status --porcelain)" ]; then revision="$revision-dirty"; fi
mkdir -p "$(dirname -- "$output")"
echo "Building and verifying Go 1.27.1 on the configured Podman remote..."
podman build --target build -f "$root/deploy/podman/Containerfile" -t "$image" "$root"
cleanup() {
  podman rm --force "$name" >/dev/null 2>&1 || true
  podman volume rm "$volume" >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM
podman volume create "$volume" >/dev/null
podman create --name "$name" --network=none --cpus=4 --memory=2g --pids-limit=512 \
  --volume "$volume:/bench-data" --env TMPDIR=/bench-data --env GOMAXPROCS=4 \
  --env JLITE_CONTINUOUS_OUTPUT=/tmp/results.json --env "JLITE_BENCH_ROUNDS=${JLITE_BENCH_ROUNDS:-3}" \
  --env "JLITE_BENCH_REVISION=$revision" \
  "$image" sh scripts/go.sh test -run '^TestContinuousBenchmark$' -count=1 -v -timeout=30m . >/dev/null
echo "Measuring live owner publication and two online replicas at 20, 80 and 200 writes/s..."
if ! podman start --attach "$name"; then
  podman cp "$name:/tmp/results.json" "$output.partial" 2>/dev/null || true
  exit 1
fi
podman cp "$name:/tmp/results.json" "$output"
podman cp "$name:/tmp/results.json.cpu" "$output.cpu"
podman cp "$name:/tmp/results.json.block" "$output.block"
echo "Results: $output (with CPU and blocking profiles)"
