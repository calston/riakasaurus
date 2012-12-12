# Copyright (c) 2008 Johan Euphrosine
# See LICENSE for details.

all: proto

proto:
	protoc -Iprotobuf --python_out=riakasaurus protobuf/riak.proto
	protoc -Iprotobuf --python_out=riakasaurus protobuf/riak_kv.proto

clean:
	find . | grep '\.pyc$$' | xargs rm -f
	find . | grep '~$$' | xargs rm -f
	find . | grep '_pb2.py$$' | xargs rm -f

.PHONY: all
