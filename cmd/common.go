package cmd

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

// CommonOption represents common options across most commands
type CommonOption struct {
	URI   string `arg:"" help:"URI of Parquet file, support s3:// and file://."`
	Debug bool   `help:"Output debug information."`
}

// Context represents command's context
type Context struct {
	Version string
	Build   string
}

// NewParquetReader returns a Parquet file reader
func newParquetFileReader(uri string) (*reader.ParquetReader, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("unable to parse file location [%s]: %s", uri, err.Error())
	}

	if u.Scheme == "" {
		u.Scheme = "file"
	}

	var fileReader source.ParquetFile
	switch u.Scheme {
	case "s3":
		// Get region of the S3 bucket
		ctx := context.Background()
		sess := session.Must(session.NewSession())
		region, err := s3manager.GetBucketRegion(ctx, sess, u.Host, "us-east-1")
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
				return nil, fmt.Errorf("unable to find bucket %s's region", u.Host)
			}
			return nil, fmt.Errorf("AWS error: %s", err.Error())
		}

		fileReader, err = s3.NewS3FileReader(ctx, u.Host, strings.TrimLeft(u.Path, "/"), &aws.Config{Region: aws.String(region)})
		if err != nil {
			return nil, fmt.Errorf("failed to open S3 object [%s]: %s", uri, err.Error())
		}
	case "file":
		fileName := filepath.Join(u.Host, u.Path)
		fileReader, err = local.NewLocalFileReader(fileName)
		if err != nil {
			return nil, fmt.Errorf("failed to open local file [%s]: %s", fileName, err.Error())
		}
	default:
		return nil, fmt.Errorf("unknown location scheme [%s]", u.Scheme)
	}

	return reader.NewParquetReader(fileReader, nil, 1)
}
