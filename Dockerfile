FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go test ./... && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /runner ./cmd/server
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /runner /runner
EXPOSE 8080
ENTRYPOINT ["/runner"]
