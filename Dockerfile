FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mock-backend ./cmd/mock-backend

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /app/
WORKDIR /app
EXPOSE 8080 9091
ENTRYPOINT ["/app/gateway"]
CMD ["-config", "/etc/gateway/gateway.yaml"]
