module github.com/hangxie/parquet-tools

go 1.16

require (
	github.com/alecthomas/kong v0.2.16
	github.com/aws/aws-sdk-go v1.38.30
	github.com/stretchr/testify v1.5.1
	github.com/xitongsys/parquet-go v1.6.0
	github.com/xitongsys/parquet-go-source v0.0.0-20201108113611-f372b7d813be
)

replace github.com/hangxie/parquet-tools/cmd => ./cmd
