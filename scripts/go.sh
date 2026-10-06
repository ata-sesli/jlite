#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
case "$(uname -s)/$(uname -m)" in
  Darwin/arm64) platform=macos-arm64 ;;
  Darwin/x86_64) platform=macos-x86_64 ;;
  Linux/aarch64|Linux/arm64) platform=linux-arm64 ;;
  Linux/x86_64) platform=linux-x86_64 ;;
  *) echo "Unsupported native setup platform" >&2; exit 1 ;;
esac
native="$root/.local/zova-v1.1.0-$platform-c-abi"
if [ ! -f "$native/include/zova.h" ] || [ ! -f "$native/lib/libzova_c.a" ]; then
  echo "Missing Zova C ABI. Run: sh scripts/setup-zova.sh" >&2
  exit 1
fi

export CGO_ENABLED=1
export CGO_CFLAGS="\"-I$native/include\"${CGO_CFLAGS:+ $CGO_CFLAGS}"
export CGO_LDFLAGS="\"-L$native/lib\"${CGO_LDFLAGS:+ $CGO_LDFLAGS}"
cd "$root"
exec go "$@"
