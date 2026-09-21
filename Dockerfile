# syntax=docker/dockerfile:1
# Multi-stage: build with the pinned toolchain, run on distroless/static.
# CGO_ENABLED=0 is what makes the binary run on distroless at all — there is no libc there.

ARG GO_VERSION=1.27.1
# --platform=$BUILDPLATFORM: the build stage always runs natively and Go
# cross-compiles for the target, so a multi-arch build does not emulate the
# compiler. arm64 went from ~11 minutes under QEMU to about one.
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION} AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-mod=readonly GOTOOLCHAIN=local
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG TARGETOS TARGETARCH
RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/collector ./cmd/collector

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/collector /usr/local/bin/collector
USER nonroot:nonroot
EXPOSE 8080/tcp 2055/udp
ENTRYPOINT ["/usr/local/bin/collector"]
