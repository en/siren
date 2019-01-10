#! /bin/sh

# $ protoc --version
# libprotoc 3.6.1

protoc --go_out=plugins=grpc,paths=source_relative:. protos/*.proto
