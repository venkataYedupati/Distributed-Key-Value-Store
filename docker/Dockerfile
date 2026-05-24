FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/server ./cmd/server

FROM alpine:3.20
WORKDIR /app
RUN addgroup -S app && adduser -S app -G app
RUN apk add --no-cache curl ca-certificates
COPY --from=build /out/server /app/server
RUN mkdir -p /data && chown -R app:app /data /app
USER app
EXPOSE 8081 9091
ENTRYPOINT ["/app/server"]
