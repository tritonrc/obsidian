# Aniani container image — statically linked, ~no base image.
#
# The binary is built OUTSIDE Docker: natively per architecture in CI (see
# .github/workflows/docker.yml), or via scripts/docker-image.sh for a local
# build. This Dockerfile only copies it in. Keeping it COPY-only is what lets
# `buildx` assemble a linux/amd64 + linux/arm64 manifest from a single runner
# without QEMU-emulating a Rust compile.
#
#   dist/amd64/aniani   (x86_64-unknown-linux-musl)
#   dist/arm64/aniani   (aarch64-unknown-linux-musl)

# Prepare a /data directory owned by the nonroot uid. `scratch` has no shell to
# mkdir/chown with, so it is staged here. Pinned to BUILDPLATFORM so this RUN
# executes natively instead of under emulation when cross-building.
FROM --platform=$BUILDPLATFORM busybox:1.37-musl AS prep
RUN mkdir -p /stage/data && chown 65532:65532 /stage/data
# Note: the COPY below must take /stage, not /stage/data. A directory source
# copies its *contents*, and the created destination would be root-owned —
# which fails snapshot writes as uid 65532.

FROM scratch

ARG TARGETARCH
COPY dist/${TARGETARCH}/aniani /aniani
COPY --from=prep --chown=65532:65532 /stage /

# HTTP API, web UI, OTLP/gRPC (h2c) and MCP all share this one port.
EXPOSE 4320

# `scratch` has no /etc/passwd, so the nonroot user is numeric — 65532 matches
# distroless' `nonroot`. /data must be writable by it for snapshots.
USER 65532:65532
WORKDIR /data
VOLUME /data

ENTRYPOINT ["/aniani"]
# 127.0.0.1 (the binary's own default) is unreachable from outside the
# container, so the image defaults to 0.0.0.0. This deliberately trips the
# "binding beyond loopback exposes an unauthenticated observability service"
# warning at startup — the container boundary is the trusted network control.
CMD ["--bind-address", "0.0.0.0", "--snapshot-dir", "/data/"]
