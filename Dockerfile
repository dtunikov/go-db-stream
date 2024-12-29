FROM --platform=$BUILDPLATFORM golang:1.23.4-alpine AS build

WORKDIR /app

COPY . .

ARG TARGETOS TARGETARCH

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o go-db-stream .

FROM alpine:latest

WORKDIR /app

# run as non-root user
RUN addgroup -g 1000 nonroot && \
  adduser -D -u 1000 -G nonroot nonroot
USER nonroot:nonroot

COPY internal/config/config.schema.json ./internal/config/
COPY --from=build /app/go-db-stream ./

ENV PATH="/app:${PATH}"

EXPOSE 8080
CMD [ "./go-db-stream" ]
