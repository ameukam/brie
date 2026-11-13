# Sample cooperative workload image for the guest supervisor protocol.
#
# The image contains only two static binaries built from this repository and
# no credentials, so it needs no base-image digest. Build the binaries first
# from the repository root, for the node's architecture:
#
#   CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -o bin/brie-counter ./cmd/brie-counter
#   CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -o bin/brie-guestctl ./cmd/brie-guestctl
#
# then build with the repository root as the context:
#
#   docker build -f examples/counter/Dockerfile .
#
# Reference the pushed image by its sha256 digest; the restricted Pod profile
# rejects tags. This file has not been built or pushed by the project.
FROM scratch

COPY bin/brie-counter /brie-counter
COPY bin/brie-guestctl /brie-guestctl
# /brie holds the durable state file and the supervisor socket on the
# container's writable root filesystem. It is owned by the unprivileged user.
COPY --chown=65532:65532 --chmod=0700 examples/counter/brie/ /brie/

USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/brie-counter"]
CMD ["--listen=:8080", "--state=/brie/counter.json", "--supervisor-socket=/brie/supervisor.sock"]
