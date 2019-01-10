FROM gcr.io/cloud-builders/go:latest as builder

RUN sed -i -e 's/v[[:digit:]]\.[[:digit:]]/edge/g' /etc/apk/repositories && \
    apk add --update --update-cache pkgconfig librdkafka-dev

WORKDIR /go/src/siren/
COPY . .

ENV GOPATH=/go
RUN GO111MODULE=on go install -v ./svc-gateway
RUN GO111MODULE=on go install -v ./svc-match
RUN GO111MODULE=on go install -v ./svc-memorystore
RUN GO111MODULE=on go install -v ./svc-sql
RUN GO111MODULE=on go install -v ./svc-wallet
RUN GO111MODULE=on go install -v ./svc-wallet/ethereum
RUN GO111MODULE=on go install -v ./svc-ws

FROM alpine:3.8

RUN apk add --update --update-cache ca-certificates && \
    update-ca-certificates

WORKDIR /siren/
COPY --from=builder /go/bin/svc-gateway .

ENTRYPOINT ["/siren/svc-gateway"]
