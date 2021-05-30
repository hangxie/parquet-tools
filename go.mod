module github.com/hangxie/parquet-tools

go 1.16

require (
	github.com/Azure/azure-storage-blob-go v0.10.0
	github.com/alecthomas/kong v0.2.16
	github.com/aws/aws-sdk-go v1.38.30
	github.com/stretchr/testify v1.6.1
	github.com/xitongsys/parquet-go v1.6.1-0.20210331075444-5ecfa15142b5
	github.com/xitongsys/parquet-go-source v0.0.0-20201108113611-f372b7d813be
)

replace github.com/hangxie/parquet-tools/cmd => ./cmd

replace google.golang.org/grpc => google.golang.org/grpc v1.29.1

replace github.com/xitongsys/parquet-go => github.com/hangxie/parquet-go v1.6.1
