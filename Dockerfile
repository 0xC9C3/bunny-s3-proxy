FROM alpine:latest AS certs
RUN apk add --no-cache ca-certificates

FROM scratch
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
ARG TARGETPLATFORM
COPY build/${TARGETPLATFORM}/bunny-s3-proxy /bunny-s3-proxy
ENTRYPOINT ["/bunny-s3-proxy"]
