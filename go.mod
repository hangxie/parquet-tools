module github.com/hangxie/parquet-tools

go 1.17

require (
	github.com/Azure/azure-storage-blob-go v0.14.0
	github.com/alecthomas/kong v0.2.16
	github.com/aws/aws-sdk-go v1.38.45
	github.com/posener/complete v1.2.3
	github.com/stretchr/testify v1.7.0
	github.com/willabides/kongplete v0.2.0
	github.com/xitongsys/parquet-go v1.6.3-0.20211225081130-7857c9514e69
	github.com/xitongsys/parquet-go-source v0.0.0-20220315005136-aec0fe3e777c
)

require (
	cloud.google.com/go v0.81.0 // indirect
	cloud.google.com/go/storage v1.15.0 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/apache/arrow/go/arrow v0.0.0-20200730104253-651201b0f516 // indirect
	github.com/apache/thrift v0.14.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/uuid v1.2.0 // indirect
	github.com/googleapis/gax-go/v2 v2.0.5 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.0.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jstemmer/go-junit-report v0.9.1 // indirect
	github.com/klauspost/compress v1.13.1 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.8 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.opencensus.io v0.23.0 // indirect
	golang.org/x/lint v0.0.0-20201208152925-83fdc39ff7b5 // indirect
	golang.org/x/mod v0.4.1 // indirect
	golang.org/x/net v0.0.0-20210316092652-d523dce5a7f4 // indirect
	golang.org/x/oauth2 v0.0.0-20210413134643-5e61552d6c78 // indirect
	golang.org/x/sys v0.0.0-20210412220455-f1c623a9e750 // indirect
	golang.org/x/text v0.3.5 // indirect
	golang.org/x/tools v0.1.0 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/api v0.45.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20210420162539-3c870d7478d2 // indirect
	google.golang.org/grpc v1.37.0 // indirect
	google.golang.org/protobuf v1.26.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c // indirect
)

replace google.golang.org/grpc => google.golang.org/grpc v1.29.1

replace github.com/xitongsys/parquet-go => github.com/xitongsys/parquet-go v1.6.3-0.20211225081130-7857c9514e69

replace github.com/dgrijalva/jwt-go => github.com/golang-jwt/jwt/v4 v4.0.0
