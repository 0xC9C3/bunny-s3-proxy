FROM scratch
ARG TARGETPLATFORM
COPY build/${TARGETPLATFORM}/bunny-s3-proxy /bunny-s3-proxy
ENTRYPOINT ["/bunny-s3-proxy"]
