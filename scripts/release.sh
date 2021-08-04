#!/bin/bash
set -x

rm -rf pkg/

CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o pkg/timber-darwin-amd64 
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o pkg/timber-linux-amd64 
CGO_ENABLED=0 GOOS=linux GOARCH=386 go build -o pkg/timber-linux-386 

pushd pkg
  for file in $(ls -1 .); do
    shafile="${file}.sha"
    tarfile="${file}.tar.gz"
    tar -czf "${tarfile}" "${file}"
    shasum -a 256 "${tarfile}" > "${shafile}"
    rm "${file}"
  done
popd