# syntax=docker/dockerfile:1
# Multi-stage: build with the pinned toolchain, run on distroless/static.
# CGO_ENABLED=0 is what makes the binary run on distroless at all — there is no libc there.

ARG GO_VERSION=1.27.1
FROM golang:${GO_VERSION} AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-mod=readonly GOTOOLCHAIN=local
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/collector ./cmd/collector

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/collector /usr/local/bin/collector
USER nonroot:nonroot
EXPOSE 8080/tcp 2055/udp
ENTRYPOINT ["/usr/local/bin/collector"]
