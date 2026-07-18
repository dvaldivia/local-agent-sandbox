# Bundled sandbox runtime image. Hermetic: the runtime is stdlib-only, so the
# build needs no module downloads. Byte-compatible with the reference Python
# runtime's HTTP API, served on 8888 rooted at /app.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /sandbox-runtime .

FROM alpine:3.20
RUN adduser -D -u 1000 app && mkdir -p /app && chown 1000:1000 /app
COPY --from=build /sandbox-runtime /usr/local/bin/sandbox-runtime
USER 1000
WORKDIR /app
ENV LASD_RUNTIME_ROOT=/app
EXPOSE 8888
ENTRYPOINT ["/usr/local/bin/sandbox-runtime"]
