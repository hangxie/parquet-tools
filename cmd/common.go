package cmd

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/xitongsys/parquet-go-source/gcs"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

// CommonOption represents common options across most commands
type CommonOption struct {
	URI string `arg:"" help:"URI of Parquet file, support s3:// and file://."`
}

// Context represents command's context
type Context struct {
	Version string
	Build   string
}

func parseURI(uri string) (*url.URL, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("unable to parse file location [%s]: %s", uri, err.Error())
	}

	if u.Scheme == "" {
		u.Scheme = "file"
	}

	if u.Scheme == "file" {
		u.Path = filepath.Join(u.Host, u.Path)
		u.Host = ""
	}

	return u, nil
}

func getBucketRegion(bucket string) (string, error) {
	// Get region of the S3 bucket
	ctx := context.Background()
	sess := session.Must(session.NewSession())
	region, err := s3manager.GetBucketRegion(ctx, sess, bucket, "us-east-1")
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			return "", fmt.Errorf("unable to find bucket %s's region", bucket)
		}
		return "", fmt.Errorf("AWS error: %s", err.Error())
	}

	return region, nil
}

func newParquetFileReader(uri string) (*reader.ParquetReader, error) {
	u, err := parseURI(uri)
	if err != nil {
		return nil, err
	}

	var fileReader source.ParquetFile
	switch u.Scheme {
	case "s3":
		region, err := getBucketRegion(u.Host)
		if err != nil {
			return nil, err
		}

		fileReader, err = s3.NewS3FileReader(context.Background(), u.Host, strings.TrimLeft(u.Path, "/"), &aws.Config{Region: aws.String(region)})
		if err != nil {
			return nil, fmt.Errorf("failed to open S3 object [%s]: %s", uri, err.Error())
		}
	case "file":
		fileReader, err = local.NewLocalFileReader(u.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to open local file [%s]: %s", u.Path, err.Error())
		}
	case "gs":
		fileReader, err = gcs.NewGcsFileReader(context.Background(), "", u.Host, strings.TrimLeft(u.Path, "/"))
		if err != nil {
			return nil, fmt.Errorf("failed to open GCS object [%s]: %s", uri, err.Error())
		}
	default:
		return nil, fmt.Errorf("unknown location scheme [%s]", u.Scheme)
	}

	return reader.NewParquetReader(fileReader, nil, int64(runtime.NumCPU()))
}

func newFileWriter(uri string) (source.ParquetFile, error) {
	u, err := parseURI(uri)
	if err != nil {
		return nil, err
	}

	var fileWriter source.ParquetFile
	switch u.Scheme {
	case "s3":
		region, err := getBucketRegion(u.Host)
		if err != nil {
			return nil, err
		}

		fileWriter, err = s3.NewS3FileWriter(context.Background(), u.Host, strings.TrimLeft(u.Path, "/"), nil, &aws.Config{Region: aws.String(region)})
		if err != nil {
			return nil, fmt.Errorf("failed to open S3 object [%s]: %s", uri, err.Error())
		}
	case "file":
		fileWriter, err = local.NewLocalFileWriter(u.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to open local file [%s]: %s", u.Path, err.Error())
		}
	case "gs":
		fileWriter, err = gcs.NewGcsFileWriter(context.Background(), "", u.Host, strings.TrimLeft(u.Path, "/"))
		if err != nil {
			return nil, fmt.Errorf("failed to open GCS object [%s]: %s", uri, err.Error())
		}
	default:
		return nil, fmt.Errorf("unknown location scheme [%s]", u.Scheme)
	}

	return fileWriter, nil
}

func newParquetFileWriter(uri string, schema interface{}) (*writer.ParquetWriter, error) {
	fileWriter, err := newFileWriter(uri)
	if err != nil {
		return nil, err
	}
	return writer.NewParquetWriter(fileWriter, schema, int64(runtime.NumCPU()))
}

func newCSVWriter(uri string, schema []string) (*writer.CSVWriter, error) {
	fileWriter, err := newFileWriter(uri)
	if err != nil {
		return nil, err
	}

	return writer.NewCSVWriter(schema, fileWriter, int64(runtime.NumCPU()))
}

func toNumber(iface interface{}) (float64, bool) {
	if v, ok := iface.(int); ok {
		return float64(v), true
	}
	if v, ok := iface.(int8); ok {
		return float64(v), true
	}
	if v, ok := iface.(int16); ok {
		return float64(v), true
	}
	if v, ok := iface.(int32); ok {
		return float64(v), true
	}
	if v, ok := iface.(int64); ok {
		return float64(v), true
	}
	if v, ok := iface.(uint); ok {
		return float64(v), true
	}
	if v, ok := iface.(uint8); ok {
		return float64(v), true
	}
	if v, ok := iface.(uint16); ok {
		return float64(v), true
	}
	if v, ok := iface.(uint32); ok {
		return float64(v), true
	}
	if v, ok := iface.(uint64); ok {
		return float64(v), true
	}
	if v, ok := iface.(float32); ok {
		return float64(v), true
	}
	if v, ok := iface.(float64); ok {
		return v, true
	}
	return 0.0, false
}
