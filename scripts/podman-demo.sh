#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
image=localhost/jlite:v0
pod=jlite-v0-demo

ctl() { node=$1; shift; podman exec "jlite-v0-$node" jlite ctl "$@"; }

wait_ready() {
  node=$1
  attempt=0
  while [ "$attempt" -lt 60 ]; do
    if ctl "$node" status 2>/dev/null | jq -e '.status.Ready == true' >/dev/null; then return; fi
    attempt=$((attempt + 1))
    if [ $((attempt % 10)) -eq 0 ]; then echo "Waiting for $node ($attempt/60)..."; fi
    sleep 1
  done
  ctl "$node" status
  echo "$node did not become ready" >&2
  exit 1
}

start_node() {
  node=$1
  if podman container exists "jlite-v0-$node"; then
    podman start "jlite-v0-$node"
  else
    podman run -d --name "jlite-v0-$node" --pod "$pod" \
      --read-only --cap-drop=all --security-opt=no-new-privileges \
      --memory=256m --pids-limit=128 \
      --tmpfs /run/jlite:rw,size=1m,mode=1777 \
      --volume "jlite-v0-$node-data:/data" \
      --secret "jlite-v0-$node,type=env,target=NATS_PASSWORD" \
      "$image" serve --node "$node"
  fi
}

case "${1:-}" in
  build)
    exec podman build -f "$root/deploy/podman/Containerfile" -t "$image" "$root"
    ;;
  up)
    command -v jq >/dev/null
    # Podman stores credentials on the configured engine. Reuse them on restart.
    for node in owner replica other; do
      if ! podman secret inspect "jlite-v0-$node" >/dev/null 2>&1; then
        echo "Creating demo credential for $node..."
        openssl rand -hex 32 | tr -d '\n' | podman secret create "jlite-v0-$node" - >/dev/null
      fi
    done
    if ! podman pod exists "$pod"; then podman pod create --name "$pod"; fi
    if podman container exists jlite-v0-broker; then
      podman start jlite-v0-broker
    else
      podman run -d --name jlite-v0-broker --pod "$pod" \
        --read-only --cap-drop=all --security-opt=no-new-privileges \
        --memory=512m --pids-limit=128 --volume jlite-v0-broker-data:/data \
        --secret jlite-v0-owner,type=env,target=OWNER_PASSWORD \
        --secret jlite-v0-replica,type=env,target=REPLICA_PASSWORD \
        --secret jlite-v0-other,type=env,target=OTHER_PASSWORD \
        "$image" broker
    fi
    start_node owner
    start_node replica
    wait_ready owner
    seed=$(ctl owner put demo-seed document seed)
    printf '%s\n' "$seed"
    sequence=$(printf '%s\n' "$seed" | jq -er '.write.Sequence')
    attempt=0
    until ctl owner status | jq -e --argjson target "$sequence" '.status.PublishedSequence >= $target' >/dev/null; do
      attempt=$((attempt+1)); [ "$attempt" -lt 30 ] || exit 1; sleep 1
    done
    wait_ready replica
    # The third node starts only after an owner write exists in the log.
    start_node other
    wait_ready other
    echo "Three jlite nodes deployed; no host ports exposed."
    ;;
  status)
    for node in owner replica other; do echo "$node local knowledge:"; ctl "$node" status; done
    ;;
  ctl)
    shift
    ctl "$@"
    ;;
  verify)
    command -v jq >/dev/null
    request="demo-$(date +%s)"
    ctl owner put "$request" document connected
    for node in replica other; do
      attempt=0
      until ctl "$node" get document | jq -e '.read.Value == "Y29ubmVjdGVk"' >/dev/null; do
        attempt=$((attempt+1)); [ "$attempt" -lt 30 ] || exit 1; sleep 1
      done
    done
    echo "Stopping only the demo broker; owner will commit an offline write..."
    podman stop --time 10 jlite-v0-broker
    # Always bring the broker back, including when a verification assertion fails.
    trap 'podman start jlite-v0-broker >/dev/null' EXIT HUP INT TERM
    ctl owner put "$request-offline" document offline
    ctl owner status | jq -e '.status.OutboxCount >= 1 and .status.LocalSequence > .status.PublishedSequence'
    for node in replica other; do
      echo "$node stale local read during broker outage:"
      ctl "$node" get document | jq -e '.read.Value == "Y29ubmVjdGVk"'
    done
    podman start jlite-v0-broker
    trap - EXIT HUP INT TERM
    for node in replica other; do
      attempt=0
      until ctl "$node" get document | jq -e '.read.Value == "b2ZmbGluZQ=="' >/dev/null; do
        attempt=$((attempt+1)); [ "$attempt" -lt 30 ] || exit 1; sleep 1
      done
    done
    echo "Reconnect and replica convergence verified."
    for node in owner replica other; do ctl "$node" status; done
    ;;
  stop)
    exec podman pod stop --time 15 "$pod"
    ;;
  *) echo "Usage: sh scripts/podman-demo.sh build|up|status|ctl node command...|verify|stop" >&2; exit 1 ;;
esac
