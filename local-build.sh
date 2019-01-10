#! /bin/sh

go build -o gateway ./svc-gateway
go build -o match ./svc-match
go build -o memorystore ./svc-memorystore
go build -o sql ./svc-sql
go build -o wallet ./svc-wallet
go build -o ethereum ./svc-wallet/ethereum
go build -o ws ./svc-ws
