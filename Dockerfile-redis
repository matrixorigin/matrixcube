FROM deepfabric/build as builder

COPY . /root/go/src/github.com/deepfabric/beehive
WORKDIR /root/go/src/github.com/deepfabric/beehive

RUN make redis

FROM deepfabric/centos
COPY --from=builder /root/go/src/github.com/deepfabric/beehive/dist/redis /usr/local/bin/redis

ENTRYPOINT ["/usr/local/bin/redis"]
