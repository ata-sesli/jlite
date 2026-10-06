#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
case "$(uname -s)/$(uname -m)" in
  Darwin/arm64) platform=macos-arm64; checksum=5d7356977a31b294bea2c0578c40ebb07de6fcb8146149c152a0ca31f294a9c7 ;;
  Darwin/x86_64) platform=macos-x86_64; checksum=c359b0b05be5b1151984438bf00d50cc2b7186e11c2110a185b84310618f7f54 ;;
  Linux/aarch64|Linux/arm64) platform=linux-arm64; checksum=d3a6926a41a4bc23da2eca2c730d0f7a321e7a494fef3237bbb1fac27f076c60 ;;
  Linux/x86_64) platform=linux-x86_64; checksum=795de410e2a2dde7fcc611ca2ca8cf9d01f2e590000fcfcb29e24d8f4bfa8464 ;;
  *) echo "No Zova 1.1.0 setup archive configured for this platform" >&2; exit 1 ;;
esac

archive="zova-v1.1.0-$platform-c-abi.tar.gz"
mkdir -p "$root/.local/downloads"
cd "$root/.local/downloads"
if [ ! -f "$archive" ]; then
  echo "Downloading Zova 1.1.0 C ABI ($platform)..."
  curl --fail --location --retry 3 --output "$archive.part" \
    "https://github.com/ata-sesli/zova/releases/download/v1.1.0/$archive"
  mv "$archive.part" "$archive"
fi

echo "Verifying Zova release archive..."
if command -v sha256sum >/dev/null 2>&1; then
  printf '%s  %s\n' "$checksum" "$archive" | sha256sum -c -
else
  printf '%s  %s\n' "$checksum" "$archive" | shasum -a 256 -c -
fi
tar -xzf "$archive" -C "$root/.local"
echo "Zova ready: $root/.local/zova-v1.1.0-$platform-c-abi"
