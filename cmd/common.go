package cmd

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	pqtazblob "github.com/xitongsys/parquet-go-source/azblob"
	"github.com/xitongsys/parquet-go-source/gcs"
	"github.com/xitongsys/parquet-go-source/hdfs"
	"github.com/xitongsys/parquet-go-source/http"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/types"
	"github.com/xitongsys/parquet-go/writer"
)

const (
	schemeLocal              string = "file"
	schemeAWSS3              string = "s3"
	schemeGoogleCloudStorage string = "gs"
	schemeAzureStorageBlob   string = "wasbs"
	schemeHTTP               string = "http"
	schemeHTTPS              string = "https"
	schemeHDFS               string = "hdfs"
)

// CommonOption represents common options across most commands
type CommonOption struct {
	URI string `arg:"" predictor:"file" help:"URI of Parquet file."`
}

// ReadOption includes options for read operation
type ReadOption struct {
	CommonOption
	HTTPMultipleConnection bool              `help:"(HTTP URI only) use multiple HTTP connection." default:"false"`
	HTTPIgnoreTLSError     bool              `help:"(HTTP URI only) ignore TLS error." default:"false"`
	HTTPExtraHeaders       map[string]string `mapsep:"," help:"(HTTP URI only) extra HTTP headers." default:""`
	ObjectVersion          string            `help:"(S3 URI only) object version." default:""`
	Anonymous              bool              `help:"(S3 and Azure only) object is publicly accessible." default:"false"`
}

// WriteOption includes options for write operation
type WriteOption struct {
	CommonOption
}

// Context represents command's context
type Context struct {
	Version string
	Build   string
}

// ReinterpretField represents a field that needs to be re-interpreted before output
type ReinterpretField struct {
	parquetType   parquet.Type
	convertedType parquet.ConvertedType
	precision     int
	scale         int
}

func parseURI(uri string) (*url.URL, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("unable to parse file location [%s]: %s", uri, err.Error())
	}

	if u.Scheme == "" {
		u.Scheme = schemeLocal
	}

	if u.Scheme == schemeLocal {
		u.Path = filepath.Join(u.Host, u.Path)
		u.Host = ""
	}

	return u, nil
}

func getS3Client(bucket string, isPublic bool) (*s3.Client, error) {
	ctx := context.TODO()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithDefaultRegion("us-east-1"))
	if err != nil {
		return nil, fmt.Errorf("failed to load config to determine bucket region: %s", err.Error())
	}
	region, err := manager.GetBucketRegion(ctx, s3.NewFromConfig(cfg), bucket)
	if err != nil {
		var apiErr manager.BucketNotFound
		if errors.As(err, &apiErr) {
			return nil, fmt.Errorf("unable to find region of bucket [%s]", bucket)
		}
		return nil, fmt.Errorf("AWS error: %s", err.Error())
	}

	if isPublic {
		return s3.NewFromConfig(aws.Config{Region: region}), nil
	}
	cfg.Region = region
	return s3.NewFromConfig(cfg), nil
}

func newLocalReader(u *url.URL, option ReadOption) (*reader.ParquetReader, error) {
	fileReader, err := local.NewLocalFileReader(u.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open local file [%s]: %s", u.Path, err.Error())
	}
	return reader.NewParquetReader(fileReader, nil, int64(runtime.NumCPU()))
}

func newAWSS3Reader(u *url.URL, option ReadOption) (*reader.ParquetReader, error) {
	s3Client, err := getS3Client(u.Host, option.Anonymous)
	if err != nil {
		return nil, err
	}

	var objVersion *string = nil
	if option.ObjectVersion != "" {
		objVersion = &option.ObjectVersion
	}
	fileReader, err := s3v2.NewS3FileReaderWithClientVersioned(context.Background(), s3Client, u.Host, strings.TrimLeft(u.Path, "/"), objVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to open S3 object [%s] version [%s]: %s", u.String(), option.ObjectVersion, err.Error())
	}
	return reader.NewParquetReader(fileReader, nil, int64(runtime.NumCPU()))
}

func newAzureStorageBlobReader(u *url.URL, option ReadOption) (*reader.ParquetReader, error) {
	azURL, cred, err := azureAccessDetail(*u, option.Anonymous)
	if err != nil {
		return nil, err
	}

	fileReader, err := pqtazblob.NewAzBlobFileReaderWithSharedKey(context.Background(), azURL, cred, azblob.ClientOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to open Azure blob object [%s]: %s", u.String(), err.Error())
	}
	return reader.NewParquetReader(fileReader, nil, int64(runtime.NumCPU()))
}

func newGoogleCloudStorageReader(u *url.URL, option ReadOption) (*reader.ParquetReader, error) {
	fileReader, err := gcs.NewGcsFileReader(context.Background(), "", u.Host, strings.TrimLeft(u.Path, "/"))
	if err != nil {
		return nil, fmt.Errorf("failed to open GCS object [%s]: %s", u.String(), err.Error())
	}
	return reader.NewParquetReader(fileReader, nil, int64(runtime.NumCPU()))
}

func newHTTPReader(u *url.URL, option ReadOption) (*reader.ParquetReader, error) {
	fileReader, err := http.NewHttpReader(u.String(), option.HTTPMultipleConnection, option.HTTPIgnoreTLSError, option.HTTPExtraHeaders)
	if err != nil {
		return nil, fmt.Errorf("failed to open HTTP source [%s]: %s", u.String(), err.Error())
	}
	return reader.NewParquetReader(fileReader, nil, int64(runtime.NumCPU()))
}

func newHDFSReader(u *url.URL, option ReadOption) (*reader.ParquetReader, error) {
	userName := u.User.Username()
	if userName == "" {
		osUser, err := user.Current()
		if err == nil && osUser != nil {
			userName = osUser.Username
		}
	}

	fileReader, err := hdfs.NewHdfsFileReader([]string{u.Host}, userName, u.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open HDFS source [%s]: %s", u.String(), err.Error())
	}
	return reader.NewParquetReader(fileReader, nil, int64(runtime.NumCPU()))
}

func newParquetFileReader(option ReadOption) (*reader.ParquetReader, error) {
	readerFuncTable := map[string]func(*url.URL, ReadOption) (*reader.ParquetReader, error){
		schemeLocal:              newLocalReader,
		schemeAWSS3:              newAWSS3Reader,
		schemeGoogleCloudStorage: newGoogleCloudStorageReader,
		schemeAzureStorageBlob:   newAzureStorageBlobReader,
		schemeHTTP:               newHTTPReader,
		schemeHTTPS:              newHTTPReader,
		schemeHDFS:               newHDFSReader,
	}

	u, err := parseURI(option.URI)
	if err != nil {
		return nil, err
	}
	if readerFunc, found := readerFuncTable[u.Scheme]; found {
		return readerFunc(u, option)
	}

	return nil, fmt.Errorf("unknown location scheme [%s]", u.Scheme)
}

func newLocalWriter(u *url.URL, option WriteOption) (source.ParquetFile, error) {
	fileWriter, err := local.NewLocalFileWriter(u.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open local file [%s]: %s", u.Path, err.Error())
	}
	return fileWriter, nil
}

func newAWSS3Writer(u *url.URL, option WriteOption) (source.ParquetFile, error) {
	s3Client, err := getS3Client(u.Host, false)
	if err != nil {
		return nil, err
	}

	fileWriter, err := s3v2.NewS3FileWriterWithClient(context.Background(), s3Client, u.Host, strings.TrimLeft(u.Path, "/"), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open S3 object [%s]: %s", u.String(), err.Error())
	}
	return fileWriter, nil
}

func newGoogleCloudStorageWriter(u *url.URL, option WriteOption) (source.ParquetFile, error) {
	fileWriter, err := gcs.NewGcsFileWriter(context.Background(), "", u.Host, strings.TrimLeft(u.Path, "/"))
	if err != nil {
		return nil, fmt.Errorf("failed to open GCS object [%s]: %s", u.String(), err.Error())
	}
	return fileWriter, nil
}

func newAzureStorageBlobWriter(u *url.URL, option WriteOption) (source.ParquetFile, error) {
	// write operation cannot be with anonymous access
	azURL, cred, err := azureAccessDetail(*u, false)
	if err != nil {
		return nil, err
	}

	fileWriter, err := pqtazblob.NewAzBlobFileWriterWithSharedKey(context.Background(), azURL, cred, azblob.ClientOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to open Azure blob object [%s]: %s", u.String(), err.Error())
	}
	return fileWriter, nil
}

func newHTTPWriter(u *url.URL, option WriteOption) (source.ParquetFile, error) {
	return nil, fmt.Errorf("writing to %s endpoint is not currently supported", u.Scheme)
}

func newHDFSWriter(u *url.URL, option WriteOption) (source.ParquetFile, error) {
	userName := u.User.Username()
	if userName == "" {
		osUser, err := user.Current()
		if err == nil && osUser != nil {
			userName = osUser.Username
		}
	}
	fileWriter, err := hdfs.NewHdfsFileWriter([]string{u.Host}, userName, u.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open HDFS source [%s]: %s", u.String(), err.Error())
	}
	return fileWriter, nil
}

func newParquetFileWriter(option WriteOption) (source.ParquetFile, error) {
	writerFuncTable := map[string]func(*url.URL, WriteOption) (source.ParquetFile, error){
		schemeLocal:              newLocalWriter,
		schemeAWSS3:              newAWSS3Writer,
		schemeGoogleCloudStorage: newGoogleCloudStorageWriter,
		schemeAzureStorageBlob:   newAzureStorageBlobWriter,
		schemeHTTP:               newHTTPWriter,
		schemeHTTPS:              newHTTPWriter,
		schemeHDFS:               newHDFSWriter,
	}

	u, err := parseURI(option.URI)
	if err != nil {
		return nil, err
	}
	if writerFunc, found := writerFuncTable[u.Scheme]; found {
		return writerFunc(u, option)
	}
	return nil, fmt.Errorf("unknown location scheme [%s]", u.Scheme)
}

func newCSVWriter(option WriteOption, schema []string) (*writer.CSVWriter, error) {
	fileWriter, err := newParquetFileWriter(option)
	if err != nil {
		return nil, err
	}

	return writer.NewCSVWriter(schema, fileWriter, int64(runtime.NumCPU()))
}

func newJSONWriter(option WriteOption, schema string) (*writer.JSONWriter, error) {
	fileWriter, err := newParquetFileWriter(option)
	if err != nil {
		return nil, err
	}

	return writer.NewJSONWriter(schema, fileWriter, int64(runtime.NumCPU()))
}

func azureAccessDetail(azURL url.URL, anonymous bool) (string, *azblob.SharedKeyCredential, error) {
	container := azURL.User.Username()
	if azURL.Host == "" || container == "" || strings.HasSuffix(azURL.Path, "/") {
		return "", nil, fmt.Errorf("azure blob URI format: wasbs://container@storageaccount.blob.core.windows.net/path/to/blob")
	}
	httpURL := fmt.Sprintf("https://%s/%s%s", azURL.Host, container, azURL.Path)

	accessKey := os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	if anonymous || accessKey == "" {
		// anonymous access
		return httpURL, nil, nil
	}

	credential, err := azblob.NewSharedKeyCredential(strings.Split(azURL.Host, ".")[0], accessKey)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create Azure credential: %v", err)
	}

	return httpURL, credential, nil
}

func getReinterpretFields(rootPath string, schemaRoot *schemaNode, noInterimLayer bool) map[string]ReinterpretField {
	reinterpretFields := make(map[string]ReinterpretField)
	for _, child := range schemaRoot.Children {
		currentPath := rootPath + "." + child.Name
		if child.Type == nil && child.ConvertedType == nil && child.NumChildren != nil {
			// STRUCT
			for k, v := range getReinterpretFields(currentPath, child, noInterimLayer) {
				reinterpretFields[k] = v
			}
			continue
		}

		if child.Type != nil && *child.Type == parquet.Type_INT96 {
			reinterpretFields[currentPath] = ReinterpretField{
				parquetType:   parquet.Type_INT96,
				convertedType: parquet.ConvertedType_TIMESTAMP_MICROS,
				precision:     0,
				scale:         0,
			}
			continue
		}

		if child.ConvertedType != nil {
			switch *child.ConvertedType {
			case parquet.ConvertedType_MAP, parquet.ConvertedType_LIST:
				if noInterimLayer {
					child = child.Children[0]
				}
				fallthrough
			case parquet.ConvertedType_MAP_KEY_VALUE:
				for k, v := range getReinterpretFields(currentPath, child, noInterimLayer) {
					reinterpretFields[k] = v
				}
			case parquet.ConvertedType_DECIMAL, parquet.ConvertedType_INTERVAL:
				reinterpretFields[currentPath] = ReinterpretField{
					parquetType:   *child.Type,
					convertedType: *child.ConvertedType,
					precision:     int(*child.Precision),
					scale:         int(*child.Scale),
				}
			}
		}
	}

	return reinterpretFields
}

func decimalToFloat(fieldAttr ReinterpretField, iface interface{}) (*float64, error) {
	if iface == nil {
		return nil, nil
	}

	switch value := iface.(type) {
	case int64:
		f64 := float64(value) / math.Pow10(fieldAttr.scale)
		return &f64, nil
	case int32:
		f64 := float64(value) / math.Pow10(fieldAttr.scale)
		return &f64, nil
	case string:
		buf := stringToBytes(fieldAttr, value)
		f64, err := strconv.ParseFloat(types.DECIMAL_BYTE_ARRAY_ToString(buf, fieldAttr.precision, fieldAttr.scale), 64)
		if err != nil {
			return nil, err
		}
		return &f64, nil
	}
	return nil, fmt.Errorf("unknown type: %s", reflect.TypeOf(iface))
}

func stringToBytes(fieldAttr ReinterpretField, value string) []byte {
	// INTERVAL uses LittleEndian, DECIMAL uses BigEndian
	// make sure all decimal-like value are all BigEndian
	buf := []byte(value)
	if fieldAttr.convertedType == parquet.ConvertedType_INTERVAL {
		for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {
			buf[i], buf[j] = buf[j], buf[i]
		}
	}
	return buf
}

type schemaNode struct {
	parquet.SchemaElement
	Parent   []string      `json:"-"`
	Children []*schemaNode `json:"children,omitempty"`
}

func newSchemaTree(reader *reader.ParquetReader) *schemaNode {
	schemas := reader.SchemaHandler.SchemaElements
	stack := []*schemaNode{}
	root := &schemaNode{
		SchemaElement: *schemas[0],
		Parent:        []string{},
		Children:      []*schemaNode{},
	}
	stack = append(stack, root)

	for pos := 1; len(stack) > 0; {
		node := stack[len(stack)-1]
		if len(node.Children) < int(node.SchemaElement.GetNumChildren()) {
			childNode := &schemaNode{
				SchemaElement: *schemas[pos],
				Parent:        append(node.Parent, node.Name),
				Children:      []*schemaNode{},
			}
			node.Children = append(node.Children, childNode)
			stack = append(stack, childNode)
			pos++
		} else {
			stack = stack[:len(stack)-1]
			if len(node.Children) == 0 {
				node.Children = nil
			}
		}
	}

	return root
}
