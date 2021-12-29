package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	pqtazblob "github.com/xitongsys/parquet-go-source/azblob"
	"github.com/xitongsys/parquet-go-source/gcs"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/types"
	"github.com/xitongsys/parquet-go/writer"
)

// CommonOption represents common options across most commands
type CommonOption struct {
	URI string `arg:"" predictor:"file" help:"URI of Parquet file, check https://github.com/hangxie/parquet-tools/blob/main/USAGE.md#parquet-file-location for more details."`
}

// Context represents command's context
type Context struct {
	Version string
	Build   string
}

// DecimalField represents a field with DECIMAL converted type
type DecimalField struct {
	parquetType parquet.Type
	precision   int
	scale       int
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
	case "wasbs":
		azURL, cred, err := azureAccessDetail(*u)
		if err != nil {
			return nil, err
		}

		fileReader, err = pqtazblob.NewAzBlobFileReader(context.Background(), azURL, cred, pqtazblob.ReaderOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to open Azure blob object [%s]: %s", uri, err.Error())
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

		fileWriter, err = s3.NewS3FileWriter(context.Background(), u.Host, strings.TrimLeft(u.Path, "/"), "bucket-owner-full-control", nil, &aws.Config{Region: aws.String(region)})
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
	case "wasbs":
		azURL, cred, err := azureAccessDetail(*u)
		if err != nil {
			return nil, err
		}

		fileWriter, err = pqtazblob.NewAzBlobFileWriter(context.Background(), azURL, cred, pqtazblob.WriterOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to open Azure blob object [%s]: %s", uri, err.Error())
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

func newJSONWriter(uri string, schema string) (*writer.JSONWriter, error) {
	fileWriter, err := newFileWriter(uri)
	if err != nil {
		return nil, err
	}

	return writer.NewJSONWriter(schema, fileWriter, int64(runtime.NumCPU()))

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

func azureAccessDetail(azURL url.URL) (string, azblob.Credential, error) {
	container := azURL.User.Username()
	if azURL.Host == "" || container == "" || strings.HasSuffix(azURL.Path, "/") {
		return "", nil, fmt.Errorf("azure blob URI format: wasbs://container@storageaccount.blob.windows.core.net/path/to/blob")
	}
	httpURL := fmt.Sprintf("https://%s/%s%s", azURL.Host, container, azURL.Path)

	accessKey := os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	if accessKey == "" {
		// anonymouse access
		return httpURL, azblob.NewAnonymousCredential(), nil
	}

	credential, err := azblob.NewSharedKeyCredential(strings.Split(azURL.Host, ".")[0], accessKey)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create Azure credential")
	}

	return httpURL, credential, nil
}

func getAllDecimalFields(rootPath string, schemaRoot *schemaNode, noInterimLayer bool) map[string]DecimalField {
	decimalFields := make(map[string]DecimalField)
	for _, child := range schemaRoot.Children {
		currentPath := rootPath + "." + child.Name
		if rootPath == "" {
			currentPath = child.Name
		}

		if child.Type == nil && child.ConvertedType == nil && child.NumChildren != nil {
			// STRUCT
			for k, v := range getAllDecimalFields(currentPath, child, noInterimLayer) {
				decimalFields[k] = v
			}
			continue
		}

		if child.ConvertedType != nil && (*child.ConvertedType == parquet.ConvertedType_MAP || *child.ConvertedType == parquet.ConvertedType_LIST) {
			if noInterimLayer {
				child = child.Children[0]
			}
			for k, v := range getAllDecimalFields(currentPath, child, noInterimLayer) {
				decimalFields[k] = v
			}
			continue
		}

		if child.ConvertedType != nil && *child.ConvertedType == parquet.ConvertedType_DECIMAL {
			decimalFields[currentPath] = DecimalField{
				parquetType: *child.Type,
				precision:   int(*child.Precision),
				scale:       int(*child.Scale),
			}
		}
	}

	return decimalFields
}

func reformatStringDecimalValue(fieldAttr DecimalField, value reflect.Value) {
	if !value.IsValid() {
		return
	}

	if value.Kind() != reflect.Ptr {
		newValue := types.DECIMAL_BYTE_ARRAY_ToString([]byte(value.String()), fieldAttr.precision, fieldAttr.scale)
		value.SetString(newValue)
		return
	}

	if !value.IsNil() {
		newValue := types.DECIMAL_BYTE_ARRAY_ToString([]byte(value.Elem().String()), fieldAttr.precision, fieldAttr.scale)
		value.Elem().SetString(newValue)
	}
}

func decimalStringToFloat64(fieldAttr DecimalField, value interface{}) (*float64, error) {
	v := reflect.ValueOf(value)
	newValue := reflect.New(v.Type()).Elem()
	newValue.Set(v)
	reformatStringDecimalValue(fieldAttr, newValue)
	decimalValue, err := strconv.ParseFloat(newValue.String(), 64)
	if err != nil {
		return nil, err
	}
	return &decimalValue, nil
}
