FROM golang:1.24-alpine AS build
WORKDIR /src
COPY . .
# go.sum генерируется при сборке; после первого `go mod tidy` локально можно коммитить его
RUN go mod tidy && go mod download
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/server ./cmd/server \
 && CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/consumer ./cmd/consumer

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/ /app/
USER app
EXPOSE 8080 9100
CMD ["/app/server"]
