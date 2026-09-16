FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/demo ./cmd/demo \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/grpc-check ./cmd/grpc-check

FROM alpine:3.23
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/ /app/
COPY configs/ /app/configs/
USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["/app/gateway"]
CMD ["-config", "/app/configs/docker.json"]
