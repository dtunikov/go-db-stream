FROM --platform=$BUILDPLATFORM golang:1.23.4-alpine AS build

WORKDIR /app

COPY . .

ARG TARGETOS TARGETARCH

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o server .

FROM alpine:latest

# run as non-root user
RUN addgroup -g 1000 nonroot && \
  adduser -D -u 1000 -G nonroot nonroot
USER nonroot:nonroot

COPY --from=build /app/server ./

EXPOSE 8080
CMD [ "./server" ]
