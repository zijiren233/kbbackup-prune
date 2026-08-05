FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /kbbackup-prune ./cmd/kbbackup-prune

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /kbbackup-prune /usr/local/bin/kbbackup-prune
ENTRYPOINT ["/usr/local/bin/kbbackup-prune"]
