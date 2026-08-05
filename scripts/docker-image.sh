#!/usr/bin/env bash
# Build an aniani container image for the host architecture, locally.
#
# CI builds each architecture on a native runner and pushes a multi-arch
# manifest (.github/workflows/docker.yml). This script does the same thing for
# one architecture using a rust:alpine container, so the Dockerfile can stay
# COPY-only and behave identically in both paths.
#
#   scripts/docker-image.sh [tag]      # default tag: aniani:dev
set -euo pipefail

tag="${1:-aniani:dev}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

case "$(uname -m)" in
  arm64 | aarch64) arch=arm64 target=aarch64-unknown-linux-musl ;;
  x86_64 | amd64) arch=amd64 target=x86_64-unknown-linux-musl ;;
  *)
    echo "unsupported host architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

echo "==> compiling $target (rust:alpine container)"
docker run --rm \
  -v "$repo_root:/src" -w /src \
  -v aniani-cargo-registry:/usr/local/cargo/registry \
  -e CARGO_TARGET_DIR=/src/target/musl \
  -e CARGO_TERM_COLOR=always \
  rust:1-alpine \
  sh -c "apk add --no-cache musl-dev >/dev/null && cargo build --release --target $target"

# The musl build must be fully static — the image has no libc to fall back on.
if ! docker run --rm -v "$repo_root:/src" busybox:1.37-musl \
  sh -c "head -c 20 /src/target/musl/$target/release/aniani | grep -q ELF"; then
  echo "expected an ELF binary at target/musl/$target/release/aniani" >&2
  exit 1
fi

mkdir -p "dist/$arch"
cp "target/musl/$target/release/aniani" "dist/$arch/aniani"
chmod +x "dist/$arch/aniani"

echo "==> building image $tag (linux/$arch)"
docker buildx build --load --platform "linux/$arch" -t "$tag" .

docker run --rm --entrypoint /aniani "$tag" --version
echo "==> $tag ready — docker run --rm -p 4320:4320 $tag"
