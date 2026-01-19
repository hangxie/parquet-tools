[![](https://img.shields.io/badge/license-BSD%203-blue)](https://github.com/hangxie/parquet-tools/blob/main/LICENSE)
[![](https://img.shields.io/github/v/tag/hangxie/parquet-tools.svg?color=brightgreen&label=version&sort=semver)](https://github.com/hangxie/parquet-tools/releases)
[![[parquet-tools]](https://github.com/hangxie/parquet-tools/actions/workflows/release.yml/badge.svg)](https://github.com/hangxie/parquet-tools/actions/workflows/release.yml)
[![](https://goreportcard.com/badge/github.com/hangxie/parquet-tools)](https://goreportcard.com/report/github.com/hangxie/parquet-tools)
[![](https://github.com/hangxie/parquet-tools/wiki/coverage.svg)](https://github.com/hangxie/parquet-tools/wiki/Coverage-Report)

# parquet-tools
A utility to inspect Parquet files.

## Quick Start

Pre-built binaries or packages can be found on the [release page](https://github.com/hangxie/parquet-tools/releases), on Mac you can install with brew:

```bash
$ brew install go-parquet-tools
```

Once it is installed:

```bash
$ parquet-tools
Usage: parquet-tools <command>

A utility to inspect Parquet files, for full usage see https://github.com/hangxie/parquet-tools/blob/main/README.md

Flags:
  -h, --help    Show context-sensitive help.

Commands:
  cat                  Prints the content of a Parquet file, data only.
  import               Create Parquet file from other source data.
  inspect              Inspect Parquet file structure in detail.
  merge                Merge multiple parquet files into one.
  meta                 Prints the metadata.
  retype               Change column type.
  row-count            Prints the count of rows.
  schema               Prints the schema.
  shell-completions    Install/uninstall shell completions
  size                 Prints the size.
  split                Split into multiple parquet files.
  transcode            Transcode Parquet file with different compression.
  version              Show build version.

Run "parquet-tools <command> --help" for more information on a command.

parquet-tools: error: expected one of "cat", "import", "inspect", "merge", "meta", "row-count", ...
```

## Table of Contents

- [parquet-tools](#parquet-tools)
  - [Quick Start](#quick-start)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
    - [Install from Source](#install-from-source)
    - [Download Pre-built Binaries](#download-pre-built-binaries)
    - [Brew Install](#brew-install)
    - [Container Image](#container-image)
    - [Prebuilt RPM and deb Packages](#prebuilt-rpm-and-deb-packages)
  - [Usage](#usage)
    - [Obtain Help](#obtain-help)
    - [Parquet File Location](#parquet-file-location)
      - [File System](#file-system)
      - [S3 Bucket](#s3-bucket)
      - [GCS Bucket](#gcs-bucket)
      - [Azure Storage Container](#azure-storage-container)
      - [HDFS File](#hdfs-file)
      - [HTTP Endpoint](#http-endpoint)
    - [File Format Options](#file-format-options)
      - [Compression Codecs](#compression-codecs)
      - [Data Page Version](#data-page-version)
      - [Writer Tuning Options](#writer-tuning-options)
      - [Encoding](#encoding)
    - [Geo Data Type Support](#geo-data-type-support)
      - [GEOGRAPHY/GEOMETRY vs GeoParquet](#geographygeometry-vs-geoparquet)
      - [Geospatial Format](#geospatial-format)
    - [Variant Data Type Support](#variant-data-type-support)
      - [VARIANT Convention](#variant-convention)
      - [Shredded VARIANT](#shredded-variant)
    - [cat Command](#cat-command)
      - [Full Data Set](#full-data-set)
      - [Skip Rows](#skip-rows)
      - [CSV/TSV Format](#csvtsv-format)
      - [Limit Number of Rows](#limit-number-of-rows)
      - [Sampling](#sampling)
      - [Compound Rule](#compound-rule)
      - [Output Format](#output-format)
    - [import Command](#import-command)
      - [Import from CSV](#import-from-csv)
      - [Import from JSON](#import-from-json)
      - [Import from JSONL](#import-from-jsonl)
    - [inspect Command](#inspect-command)
      - [Inspect File Level](#inspect-file-level)
      - [Inspect Row Group Level](#inspect-row-group-level)
      - [Inspect Column Chunk Level](#inspect-column-chunk-level)
      - [Inspect Page Level](#inspect-page-level)
    - [merge Command](#merge-command)
    - [meta Command](#meta-command)
      - [Show Meta Data](#show-meta-data)
    - [retype Command](#retype-command)
      - [Convert INT96 to Timestamp](#convert-int96-to-timestamp)
      - [Convert BSON to String](#convert-bson-to-string)
      - [Remove JSON Logical Type](#remove-json-logical-type)
      - [Convert FLOAT16 to FLOAT32](#convert-float16-to-float32)
    - [row-count Command](#row-count-command)
      - [Show Number of Rows](#show-number-of-rows)
    - [schema Command](#schema-command)
      - [JSON Format](#json-format)
      - [Raw Format](#raw-format)
      - [Go Struct Format](#go-struct-format)
      - [CSV Format](#csv-format)
    - [shell-completions Command (Experimental)](#shell-completions-command-experimental)
      - [Install Shell Completions](#install-shell-completions)
      - [Uninstall Shell Completions](#uninstall-shell-completions)
      - [Use Shell Completions](#use-shell-completions)
    - [size Command](#size-command)
      - [Show Raw Size](#show-raw-size)
      - [Show Footer Size in JSON Format](#show-footer-size-in-json-format)
      - [Show All Sizes in JSON Format](#show-all-sizes-in-json-format)
    - [split Command](#split-command)
      - [Name format](#name-format)
      - [Exact number of output files](#exact-number-of-output-files)
      - [Maximum records in a file](#maximum-records-in-a-file)
    - [transcode Command](#transcode-command)
      - [Change Compression Codec](#change-compression-codec)
      - [Change Data Page Version](#change-data-page-version)
      - [Field-Specific Encoding](#field-specific-encoding)
      - [Control Statistics](#control-statistics)
      - [Field-Specific Compression](#field-specific-compression)
      - [Combine Multiple Options](#combine-multiple-options)
      - [INT96 Field Detection](#int96-field-detection)
    - [version Command](#version-command)
      - [Print Version](#print-version)
      - [Print All Information](#print-all-information)
      - [Print Version and Build Time in JSON Format](#print-version-and-build-time-in-json-format)
      - [Print Version in JSON Format](#print-version-in-json-format)
  - [Credit](#credit)
  - [License](#license)

## Installation

You can choose one of the installation methods from below, the functionality will be mostly the same.

### Install from Source

Good for people who are familiar with [Go](https://go.dev/), you need 1.24 or newer version.

```bash
$ go install github.com/hangxie/parquet-tools@latest
```

The above command installs the latest released version of `parquet-tools` to $GOPATH/bin, `parquet-tools` installed from source will not report proper version and build time, so if you run `parquet-tools version`, it will just give you an empty line, all other functions are not affected.

> [!TIP]
> If you do not set `GOPATH` environment variable explicitly, then its default value can be obtained by running `go env GOPATH`, usually it is `go/` directory under your home directory.

### Download Pre-built Binaries

Good for people who do not want to build and all other installation approaches do not work.

Go to [release page](https://github.com/hangxie/parquet-tools/releases), pick the release and platform you want to run, download the corresponding gz/zip file, extract it to your local disk, make sure the execution bit is set if you are running on Linux, Mac, or FreeBSD, then run the program.

For Windows 10 on ARM (like Surface Pro X), use windows-arm64, if you are using Windows 11 on ARM, both windows-arm64 and windows-amd64 build should work.

### Brew Install

Mac users can use [Homebrew](https://brew.sh/) to install:

```bash
$ brew install go-parquet-tools
```

To upgrade to the latest version:

```bash
$ brew upgrade go-parquet-tools
```

### Container Image

Container image supports amd64, arm64, and arm/v7, it is hosted in two registries:

* [Docker Hub](https://hub.docker.com/r/hangxie/parquet-tools)
* [GitHub Packages](https://github.com/users/hangxie/packages/container/package/parquet-tools)

You can pull the image from either location:

```bash
$ docker run --rm hangxie/parquet-tools version
v1.45.0
$ podman run --rm ghcr.io/hangxie/parquet-tools version
v1.45.0
```

### Prebuilt RPM and deb Packages

RPM and deb package can be found on [release page](https://github.com/hangxie/parquet-tools/releases), only amd64/x86_64 and arm64/aarch64 arch are available at this moment, download the proper package and run corresponding installation command:

* On Debian/Ubuntu:

```bash
$ sudo dpkg -i parquet-tools_1.45.0_amd64.deb
Preparing to unpack parquet-tools_1.45.0_amd64.deb ...
Unpacking parquet-tools (1.45.0) ...
Setting up parquet-tools (1.45.0) ...
```

* On CentOS/Fedora:

```bash
$ sudo rpm -Uhv parquet-tools-1.45.0-1.x86_64.rpm
Verifying...                         ################################# [100%]
Preparing...                         ################################# [100%]
Updating / installing...
   1:parquet-tools-1.45.0-1          ################################# [100%]
```

## Usage

### Obtain Help
`parquet-tools` provides help information through `-h` flag, whenever you are not sure about a parameter for a command, just add `-h` to the end of the line then it will give you all available options, for example:

```bash
$ parquet-tools meta -h
Usage: parquet-tools meta <uri> [flags]

Prints the metadata.

Arguments:
  <uri>    URI of Parquet file.

Flags:
  -h, --help                        Show context-sensitive help.

      --fail-on-int96               fail command if INT96 data type is present.
      --anonymous                   (S3, GCS, and Azure only) object is publicly accessible.
      --http-extra-headers=         (HTTP URI only) extra HTTP headers.
      --http-ignore-tls-error       (HTTP URI only) ignore TLS error.
      --http-multiple-connection    (HTTP URI only) use multiple HTTP connection.
      --object-version=""           (S3, GCS, and Azure only) object version.
```

Most commands can output JSON format result which can be processed by utilities like [jq](https://stedolan.github.io/jq/) or [JSON parser online](https://jsonparseronline.com/).

### Parquet File Location

`parquet-tools` can read and write parquet files from these locations:
* file system
* AWS Simple Storage Service (S3) bucket
* Google Cloud Storage (GCS) bucket
* Azure Storage Container
* HDFS file

`parquet-tools` can read parquet files from these locations:
* HTTP/HTTPS URL

> [!IMPORTANT]
> You need to have proper permission on the file you are going to process.

#### File System

For files from the file system, you can specify `file://` scheme or just ignore it:

```bash
$ parquet-tools row-count testdata/good.parquet
3
$ parquet-tools row-count file://testdata/good.parquet
3
$ parquet-tools row-count file://./testdata/good.parquet
3
```

#### S3 Bucket

Use full S3 URL to indicate S3 object location, it starts with `s3://`. You need to make sure you have permission to read or write the S3 object, the easiest way to verify that is using [AWS cli](https://aws.amazon.com/cli/):

```bash
$ aws sts get-caller-identity
{
    "UserId": "REDACTED",
    "Account": "123456789012",
    "Arn": "arn:aws:iam::123456789012:user/redacted"
}
aws s3 ls s3://daylight-openstreetmap/parquet/osm_features/release=v1.46/type=way/20240506_151445_00143_nanmw_fb5fe2f1-fec8-494f-8c2e-0feb15cedff0
2024-05-06 08:33:48  362267322 20240506_151445_00143_nanmw_fb5fe2f1-fec8-494f-8c2e-0feb15cedff0
$ parquet-tools row-count s3://daylight-openstreetmap/parquet/osm_features/release=v1.46/type=way/20240506_151445_00143_nanmw_fb5fe2f1-fec8-494f-8c2e-0feb15cedff0
2405462
```

If an S3 object is publicly accessible and you do not have AWS credential, you can use `--anonymous` flag to bypass AWS authentication:

```bash
$ aws sts get-caller-identity

Unable to locate credentials. You can configure credentials by running "aws configure".
$ aws s3 --no-sign-request ls s3://daylight-openstreetmap/parquet/osm_features/release=v1.46/type=way/20240506_151445_00143_nanmw_fb5fe2f1-fec8-494f-8c2e-0feb15cedff0
2024-05-06 08:33:48  362267322 20240506_151445_00143_nanmw_fb5fe2f1-fec8-494f-8c2e-0feb15cedff0
$ parquet-tools row-count --anonymous s3://daylight-openstreetmap/parquet/osm_features/release=v1.46/type=way/20240506_151445_00143_nanmw_fb5fe2f1-fec8-494f-8c2e-0feb15cedff0
2405462
```

Optionally, you can specify object version by using `--object-version` when you perform read operation (like cat, row-count, schema, etc.) for S3, `parquet-tools` will access current version if this parameter is omitted.

If version for the S3 object does not exist or bucket does not have version enabled, `parquet-tools` will report error:

```bash
$ parquet-tools row-count s3://daylight-openstreetmap/parquet/osm_features/release=v1.46/type=way/20240506_151445_00143_nanmw_fb5fe2f1-fec8-494f-8c2e-0feb15cedff0 --object-version non-existent-version
parquet-tools: error: failed to open S3 object [s3://daylight-openstreetmap/parquet/osm_features/release=v1.46/type=way/20240506_151445_00143_nanmw_fb5fe2f1-fec8-494f-8c2e-0feb15cedff0] version [non-existent-version]: operation error S3: HeadObject, https response error StatusCode: 400, RequestID: 75GZZ1W5M4KMAK1H, HostID: hgDGBOolDqLgH+CHRuZU+dXZXv4CB+mmSpjEfGxF5fLnKhNkJCWEAZBSS0kbT/k2gFotuoWNLX+zaWNWzHR49w==, api error BadRequest: Bad Request
```

> [!TIP]
> According to [HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html) and [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html), status code for non-existent object or version will be 403 instead of 404 if the caller does not have permission to `ListBucket`, or return 400 if bucket does not have version enabled.

Thanks to [parquet-go-source](https://github.com/xitongsys/parquet-go-source), `parquet-tools` loads only necessary data from S3 bucket, for most cases it is footer only, so it is much more faster than downloading the file from S3 bucket and run `parquet-tools` on a local file. Size of the S3 object used in above sample is more than 4GB, but the `row-count` command takes just several seconds to finish.

#### GCS Bucket

Use full [gsutil](https://cloud.google.com/storage/docs/gsutil) URI to point to GCS object location, it starts with `gs://`. You need to make sure you have permission to read or write to the GCS object, either use application default or GOOGLE_APPLICATION_CREDENTIALS, you can refer to [Google Cloud document](https://cloud.google.com/docs/authentication/production#automatically) for more details.

```bash
$ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service/account/key.json
$ parquet-tools import -s testdata/csv.source -m testdata/csv.schema gs://REDACTED/csv.parquet
$ parquet-tools row-count gs://REDACTED/csv.parquet
7
```

Similar to S3, `parquet-tools` downloads only necessary data from GCS bucket.

If the GCS object is publicly accessible, you can use `--anonymous` option to indicate that anonymous access is expected:

```bash
$ parquet-tools row-count gs://cloud-samples-data/bigquery/us-states/us-states.parquet
parquet-tools: error: failed to create GCS client: dialing: google: could not find default credentials. See https://cloud.google.com/docs/authentication/external/set-up-adc for more information
$ parquet-tools row-count --anonymous gs://cloud-samples-data/bigquery/us-states/us-states.parquet
50
```

Optionally, you can specify object generation by using `--object-version` when you perform read operation (like cat, row-count, schema, etc.), `parquet-tools` will access latest generation if this parameter is omitted.

```bash
$ parquet-tools row-count --anonymous gs://cloud-samples-data/bigquery/us-states/us-states.parquet
50
$ parquet-tools row-count --anonymous --object-version=-1 gs://cloud-samples-data/bigquery/us-states/us-states.parquet
50
```

`parquet-tools` reports error on invalid or non-existent generations:

```bash
$ parquet-tools row-count --anonymous --object-version=123 gs://cloud-samples-data/bigquery/us-states/us-states.parquet
parquet-tools: error: unable to open file [gs://cloud-samples-data/bigquery/us-states/us-states.parquet]: failed to create new reader: storage: object doesn't exist: googleapi: Error 404: No such object: cloud-samples-data/bigquery/us-states/us-states.parquet, notFound
$ parquet-tools row-count --anonymous --object-version=foo-bar gs://cloud-samples-data/bigquery/us-states/us-states.parquet
parquet-tools: error: unable to open file [gs://cloud-samples-data/bigquery/us-states/us-states.parquet]: invalid GCS generation [foo-bar]: strconv.ParseInt: parsing "foo-bar": invalid syntax
```

#### Azure Storage Container

`parquet-tools` uses the [HDFS URL format](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-use-blob-storage#access-files-from-within-cluster):
* starts with `wasbs://` (`wasb://` is not supported), followed by
* container as user name, followed by
* storage account as host, followed by
* blob name as path

For example:

> wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet

means the parquet file is at:
* storage account `azureopendatastorage`
* container `laborstatisticscontainer`
* blob `lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet`

`parquet-tools` uses `AZURE_STORAGE_ACCESS_KEY` environment variable to identify access:

```bash
$ AZURE_STORAGE_ACCESS_KEY=REDACTED parquet-tools import -s testdata/csv.source -m testdata/csv.schema wasbs://REDACTED@REDACTED.blob.core.windows.net/test/csv.parquet
$ AZURE_STORAGE_ACCESS_KEY=REDACTED parquet-tools row-count wasbs://REDACTED@REDACTED.blob.core.windows.net/test/csv.parquet
7
```

If the blob is publicly accessible, either unset `AZURE_STORAGE_ACCESS_KEY` or use `--anonymous` option to indicate that anonymous access is expected:

```bash
$ AZURE_STORAGE_ACCESS_KEY= parquet-tools row-count wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet
6582726
$ parquet-tools row-count --anonymous wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet
6582726
```

Optionally, you can specify object version by using `--object-version` when you perform read operation (like cat, row-count, schema, etc.) for Azure blob, `parquet-tools` will access current version if this parameter is omitted.

> [!NOTE]
> Azure blob returns different errors for non-existent version and invalid version id:
```bash
$ parquet-tools row-count --anonymous wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet --object-version foo-bar
parquet-tools: error: unable to open file [wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet]: HEAD https://azureopendatastorage.blob.core.windows.net/laborstatisticscontainer/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet
                      --------------------------------------------------------------------------------
                      RESPONSE 400: 400 Value for one of the query parameters specified in the request URI is invalid.
                      ERROR CODE UNAVAILABLE
                      --------------------------------------------------------------------------------
                      Response contained no body
                      --------------------------------------------------------------------------------
$ parquet-tools row-count --anonymous wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet --object-version 2025-05-20T01:27:08.0552942Z
parquet-tools: error: unable to open file [wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet]: HEAD https://azureopendatastorage.blob.core.windows.net/laborstatisticscontainer/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet
                      --------------------------------------------------------------------------------
                      RESPONSE 404: 404 The specified blob does not exist.
                      ERROR CODE: BlobNotFound
                      --------------------------------------------------------------------------------
                      Response contained no body
                      --------------------------------------------------------------------------------
```

Similar to S3 and GCS, `parquet-tools` downloads only necessary data from blob.

#### HDFS File

`parquet-tools` can read and write files under HDFS with schema `hdfs://username@hostname:port/path/to/file`, if `username` is not provided then current OS user will be used.

```bash
$ parquet-tools import -f jsonl -m testdata/jsonl.schema -s testdata/jsonl.source hdfs://localhost:9000/temp/good.parquet
parquet-tools: error: failed to create JSON writer: failed to open HDFS source [hdfs://localhost:9000/temp/good.parquet]: create /temp/good.parquet: permission denied
$ parquet-tools import -f jsonl -m testdata/jsonl.schema -s testdata/jsonl.source hdfs://root@localhost:9000/temp/good.parquet
$ parquet-tools row-count hdfs://localhost:9000/temp/good.parquet
7
```

Similar to cloud storage, `parquet-tools` downloads only necessary data from HDFS.

#### HTTP Endpoint

`parquet-tools` supports URI with `http` or `https` scheme, the remote server needs to support [Range header](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range), particularly with unit of `bytes`.

HTTP endpoint does not support write operation so it cannot be used as destination of `import`, `merge`, or `split` command.

These options can be used along with HTTP endpoints:
* `--http-multiple-connection` will enable dedicated transport for concurrent requests, `parquet-tools` will establish multiple TCP connections to remote server. This may or may not have performance impact depending on how remote server handles concurrent connections, it is recommended to leave it to default `false` value for all commands except `cat`, and test performance carefully with `cat` command.
* `--http-extra-headers` in the format of `key1=value1,key2=value2,...`, they will be used as extra HTTP headers, a use case is to provide `Authorization` header or JWT token that is required by remote server.
* `--http-ignore-tls-error` will ignore TLS errors, this is generally a bad idea.

```bash
$ parquet-tools row-count https://azureopendatastorage.blob.core.windows.net/laborstatisticscontainer/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet
6582726
$ parquet-tools size https://dpla-provider-export.s3.amazonaws.com/2021/04/all.parquet/part-00000-471427c6-8097-428d-9703-a751a6572cca-c000.snappy.parquet
4632041101
```

Similar to S3 and other remote endpoints, `parquet-tools` downloads only necessary data from remote server through [Range header](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range).

> [!TIP]
> `parquet-tools` will use HTTP/2 if remote server supports this, however you can disable this if things are not working well by setting environment variable `GODEBUG` to `http2client=0`:

```bash
$ parquet-tools row-count https://...
2022/09/05 09:54:52 protocol error: received DATA after END_STREAM
2022/09/05 09:54:52 protocol error: received DATA after END_STREAM
2022/09/05 09:54:53 protocol error: received DATA after END_STREAM
2022/09/05 09:54:53 protocol error: received DATA after END_STREAM
2022/09/05 09:54:53 protocol error: received DATA after END_STREAM
2022/09/05 09:54:53 protocol error: received DATA after END_STREAM
2022/09/05 09:54:53 protocol error: received DATA after END_STREAM
2022/09/05 09:54:53 protocol error: received DATA after END_STREAM
2022/09/05 09:54:53 protocol error: received DATA after END_STREAM
18141856

$ GODEBUG=http2client=0 parquet-tools row-count https://...
18141856
```

### File Format Options

This section describes format options for commands that write Parquet files, including compression, data page version, and encoding settings.

#### Compression Codecs

The `--compression` / `-z` parameter controls the compression algorithm used for writing Parquet files. This option is available for `import`, `merge`, `split`, and `transcode` commands. Different compression codecs offer different trade-offs between compression ratio, speed, and compatibility.

**Supported compression codecs:**
* `BROTLI` - Excellent compression ratio with moderate speed
* `GZIP` - Better compression ratio, slower than SNAPPY
* `LZ4_RAW` - LZ4 raw format, fast compression/decompression
* `SNAPPY` - Fast compression/decompression (default for most commands)
* `UNCOMPRESSED` - No compression
* `ZSTD` - Excellent compression ratio with good speed

**Deprecated compression codecs:**
* `LZ4` - Deprecated in Parquet format. Use `LZ4_RAW` instead for LZ4 compression

**Unsupported codecs:**
* `LZO` - Not supported. See [parquet-go#150](https://github.com/hangxie/parquet-go/issues/150) for details.

> [!TIP]
> Choose compression codecs based on your use case:
> - **SNAPPY** (default): Best for balanced performance and moderate compression
> - **ZSTD** or **BROTLI**: Best for storage optimization when compression ratio is priority
> - **GZIP**: Good compression ratio with wide compatibility
> - **LZ4_RAW**: Best for write-heavy workloads requiring maximum speed
> - **UNCOMPRESSED**: Best for debugging or when data is already compressed

#### Data Page Version

Use `--data-page-version` to specify the data page format version. This option is available for `import`, `merge`, `split`, and `transcode` commands.

Data page version 2 is preferred as it offers better compression efficiency and read performance by separating repetition/definition levels from the data. However, version 1 has wider support among older Parquet readers.

**Supported versions:**
* `1` - DATA_PAGE format (legacy, compatible with older readers)
* `2` - DATA_PAGE_V2 format (default, more efficient, requires Parquet format v2.0 readers)

#### Writer Tuning Options

These options allow fine-grained control over how Parquet files are written. They are available for `import`, `merge`, `split`, and `transcode` commands.

**Page Size (`--page-size`):**

Controls the target size of data pages in bytes. Smaller pages allow more granular access but increase metadata overhead. Default is `1048576` (1 MB).

```bash
$ parquet-tools import -f csv -s data.csv -m schema.json --page-size 524288 output.parquet
```

**Row Group Size (`--row-group-size`):**

Controls the target size of row groups in bytes. Larger row groups improve compression and reduce metadata overhead, but require more memory during read/write operations. Default is `134217728` (128 MB).

```bash
$ parquet-tools transcode -s input.parquet --row-group-size 268435456 output.parquet
```

**Parallel Number (`--parallel-number`):**

Controls the number of parallel writer goroutines. Set to `0` (default) to use the number of CPU cores. Higher values can improve write performance on systems with many cores.

```bash
$ parquet-tools merge -s file1.parquet,file2.parquet --parallel-number 4 output.parquet
```

> [!TIP]
> - Use smaller `--page-size` for better random access performance at the cost of higher metadata overhead
> - Use larger `--row-group-size` for better compression ratios, but ensure sufficient memory is available
> - Adjust `--parallel-number` based on your system's CPU cores and I/O capabilities

#### Encoding

Parquet supports various encodings for different data types. Encoding can be specified in the schema file (for `import` command) or via `--field-encoding` option (for `transcode` command).

**Supported encodings and compatible types:**

| Encoding                  | Compatible Types                                  | Description                                                         |
| ------------------------- | ------------------------------------------------- | ------------------------------------------------------------------- |
| `PLAIN`                   | All types                                         | Default encoding, no compression                                    |
| `RLE`                     | BOOLEAN, INT32, INT64                             | Run-length encoding for repeated values                             |
| `BIT_PACKED`              | BOOLEAN, INT32, INT64                             | Deprecated, use RLE instead                                         |
| `DELTA_BINARY_PACKED`     | INT32, INT64                                      | Delta encoding for sorted integers                                  |
| `DELTA_BYTE_ARRAY`        | BYTE_ARRAY                                        | Delta encoding for strings                                          |
| `DELTA_LENGTH_BYTE_ARRAY` | BYTE_ARRAY                                        | Delta encoding for variable-length byte arrays                      |
| `BYTE_STREAM_SPLIT`       | FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY | Byte interleaving for floating-point data                           |
| `RLE_DICTIONARY`          | All types                                         | Dictionary encoding with RLE, efficient for low-cardinality data    |
| `PLAIN_DICTIONARY`        | All types                                         | Dictionary encoding (v1 data pages only, use RLE_DICTIONARY for v2) |

> [!NOTE]
> Encodings must be compatible with the field type. Specifying an incompatible encoding will result in an error.

### Geo Data Type Support

> [!WARNING]
> This is an experimental feature that still under development, functionalities may be changed in the future.

`parquet-tools` recognize `GEOGRAPHY` and `GEOMETRY` logical types:

```bash
$ parquet-tools schema --format go testdata/geospatial.parquet
type Parquet_go_root struct {
	Geometry  string `parquet:"name=Geometry, type=BYTE_ARRAY, logicaltype=GEOMETRY, encoding=PLAIN"`
	Geography string `parquet:"name=Geography, type=BYTE_ARRAY, logicaltype=GEOGRAPHY, encoding=PLAIN"`
}
```

#### GEOGRAPHY/GEOMETRY vs GeoParquet
`parquet-tool` support `GEOGRAPHY` and `GEOMETRY` logical types, these types were introduced in [Apache Parquet Format 2.11.0](https://github.com/apache/parquet-format/releases/tag/apache-parquet-format-2.11.0). However, `parquet-tools` does not support [GeoParquet format](https://geoparquet.org/) as it does not provide schema information in parquet file itself, those fields in GeoParquet format file are just `BYTE_ARRAY`.

#### Geospatial Format

`parquet-tools` support different output formats for `GEOGRAPHY` and `GEOMETRY` types:
* `geojson`: output in [GeoJSON](https://datatracker.ietf.org/doc/html/rfc7946) format
* `hex`: output raw data in hex format, plus crs/algorithm
* `base64`: output raw data in base64 format, plus crs/algorithm

You can use `--geo-format` option to change format of `cat` command output, default is `geojson`.

```bash
$ parquet-tools cat --limit 1 testdata/geospatial.parquet
[{"Geography":{"geometry":{"coordinates":[0,0],"type":"Point"},"properties":{"algorithm":"SPHERICAL","crs":"OGC:CRS84"},"type":"Feature"},"Geometry":{"geometry":{"coordinates":[0,0],"type":"Point"},"properties":{"crs":"OGC:CRS84"},"type":"Feature"}}]

$ parquet-tools cat --limit 1 --geo-format geojson testdata/geospatial.parquet
[{"Geography":{"geometry":{"coordinates":[0,0],"type":"Point"},"properties":{"algorithm":"SPHERICAL","crs":"OGC:CRS84"},"type":"Feature"},"Geometry":{"geometry":{"coordinates":[0,0],"type":"Point"},"properties":{"crs":"OGC:CRS84"},"type":"Feature"}}]

$ parquet-tools cat --limit 1 --geo-format hex testdata/geospatial.parquet
[{"Geography":{"algorithm":"SPHERICAL","crs":"OGC:CRS84","wkb_hex":"010100000000000000000000000000000000000000"},"Geometry":{"crs":"OGC:CRS84","wkb_hex":"010100000000000000000000000000000000000000"}}]
```

`MinValue` and `MaxValue` of geospatial columns will be bounding box value if Geospatial Statistics presents, note that `MinValue` and `MaxValue` of underlying `BYTE_ARRAY` value do not make any sense to these columns.

```bash
$ parquet-tools meta testdata/geospatial.parquet
{"NumRowGroups":1,"RowGroups":[{"NumRows":10,"TotalByteSize":1774,"Columns":[{"PathInSchema":["Geometry"],"Type":"BYTE_ARRAY","LogicalType":"logicaltype=GEOMETRY","Encodings":["PLAIN","RLE"],"CompressedSize":422,"UncompressedSize":974,"NumValues":10,"NullCount":0,"MaxValue":[16,11],"MinValue":[-3,-8],"CompressionCodec":"SNAPPY"},{"PathInSchema":["Geography"],"Type":"BYTE_ARRAY","LogicalType":"logicaltype=GEOGRAPHY","Encodings":["PLAIN","RLE"],"CompressedSize":393,"UncompressedSize":800,"NumValues":10,"CompressionCodec":"SNAPPY"}]}]}
```

### Variant Data Type Support

The `VARIANT` logical type represents semi-structured data, similar to JSON but in a more efficient binary format. It was introduced in newer Parquet format versions to provide better performance for semi-structured data.

#### Physical Storage

Physically, a `VARIANT` field must be a group (struct) containing two required binary fields:
* `metadata`: `BYTE_ARRAY` containing the dictionary for the variant.
* `value`: `BYTE_ARRAY` containing the actual data.

#### Logical View

`parquet-tools` abstracts away the physical storage details to provide a convenient logical view of the data.

**Go Structs:**

`parquet-tools` generates `any` for `VARIANT` columns, allowing seamless decoding of the semi-structured data:

```go
type MyRecord struct {
	Data any `parquet:"name=Data, type=VARIANT, logicaltype=VARIANT"`
}
```

**JSON Schema:**

Similarly, in the JSON schema format, the internal `metadata` and `value` fields are suppressed:

```json
{
  "Tag": "name=Data, type=VARIANT, logicaltype=VARIANT"
}
```

#### Advanced: Fine-Grained Compression

When using the `any` type (Logical View), any encoding or compression settings applied to the field are inherited by both underlying `metadata` and `value` columns.

If you need granular control (e.g., using different compression codecs for `metadata` vs. `value`), you can define a custom struct that explicitly maps to the physical storage structure. Please refer to [this example](https://github.com/hangxie/parquet-go/blob/main/example/variant-fine-control/variant-fine-control.go) for more details.

#### Shredded VARIANT

"Shredding" is a technique where common paths within a `VARIANT` are extracted into separate columns to improve query performance and compression.

* **Reading**: `parquet-tools cat` and other read commands automatically support reading shredded variants. The tool will reconstruct the original semi-structured value from the shredded columns and the base variant column.
* **Writing**: Current version of `parquet-tools` (via `import`, `merge`, etc.) does not support writing shredded variants. It will always write the `VARIANT` data as a single base column containing the `metadata` and `value` fields.

### cat Command

`cat` command outputs data in parquet file, it supports JSON, JSONL, CSV, and TSV format. Since most parquet files are rather large, you can use `row-count` command to have a rough idea how many rows are there in the parquet file, then use `--skip`, `--limit` and `--sample-ratio` flags to reduce the output to a certain level, these flags can be used together.

There is a parameter that you probably will never touch: `--read-page-size` tells how many rows `parquet-tools` needs to read from the parquet file every time, you can play with it if you hit performance or resource problem.

#### Full Data Set

```bash
$ parquet-tools cat --format jsonl testdata/good.parquet
{"shoe_brand":"nike","shoe_name":"air_griffey"}
{"shoe_brand":"fila","shoe_name":"grant_hill_2"}
{"shoe_brand":"steph_curry","shoe_name":"curry7"}
```

> [!TIP]
> You can set `--fail-on-int96` option to fail `cat` command for parquet files that contain fields with INT96 type, which is [deprecated](https://issues.apache.org/jira/browse/PARQUET-323), default value for this option is `false` so you can still read INT96 type, but this behavior may change in the future.

```bash
$ parquet-tools cat --fail-on-int96 testdata/all-types.parquet
parquet-tools: error: field Int96 has type INT96 which is not supported
$ parquet-tools cat testdata/all-types.parquet
[{"Bool":true,"ByteArray":"ByteArray-0","Date":1640995200,...
```

#### Skip Rows

`--skip` is similar to OFFSET in SQL, `parquet-tools` will skip this many rows from the beginning of the parquet file before applying other logic.

```bash
$ parquet-tools cat --skip 2 --format jsonl testdata/good.parquet
{"shoe_brand":"steph_curry","shoe_name":"curry7"}
```

> [!CAUTION]
> `parquet-tools` will not report error if `--skip` is greater than total number of rows in parquet file.

```bash
$ parquet-tools cat --skip 20 testdata/good.parquet
[]
```

#### CSV/TSV Format

> [!WARNING]
> `parquet-tools` uses Go's encoding/csv library for CSV output. Since this library is [not fully compliant](https://pkg.go.dev/encoding/csv#pkg-overview) with the [RFC 4180](https://datatracker.ietf.org/doc/html/rfc4180) CSV standard, there's no guarantee that the resulting CSV file will be correctly interpreted by other data analysis tools, especially those written in different programming languages. There is no wide-accepted TSV standard so it may have more compatibility problems.

```bash
$ parquet-tools cat -f csv testdata/good.parquet
shoe_brand,shoe_name
nike,air_griffey
fila,grant_hill_2
steph_curry,curry7
```

> [!CAUTION]
> `nil` values will be presented as empty string:

```bash
$ parquet-tools cat -f csv --limit 2 testdata/int96-nil-min-max.parquet
Utf8,Int96
UTF8-0,
UTF8-1,
```

By default CSV and TSV output contains a header line with field names, you can use `--no-header` option to remove it from output.

```bash
$ parquet-tools cat -f csv --no-header testdata/good.parquet
nike,air_griffey
fila,grant_hill_2
steph_curry,curry7
```

> [!IMPORTANT]
> CSV and TSV do not support parquet files with complex schema:

```bash
$ parquet-tools cat -f csv testdata/all-types.parquet
parquet-tools: error: field [Map] is not scalar type, cannot output in csv format
```

#### Limit Number of Rows

`--limit` is similar to LIMIT in SQL, or `head` in Linux shell, `parquet-tools` will stop running after outputting this many rows.

```bash
$ parquet-tools cat --limit 2 testdata/good.parquet
[{"shoe_brand":"nike","shoe_name":"air_griffey"},{"shoe_brand":"fila","shoe_name":"grant_hill_2"}]
```

#### Sampling

`--sample-ratio` enables sampling, the ratio is a number between 0.0 and 1.0 inclusively. `1.0` means output everything in the parquet file, while `0.0` means nothing. If you want to have 1 row out of every 10 rows, use `0.1`.

> [!CAUTION]
> This feature picks rows in parquet file randomly, so only `0.0` and `1.0` will output deterministic result, all other ratio may generate data set less or more than you want.

```bash
$ parquet-tools cat --sample-ratio 0.34 testdata/good.parquet
[{"shoe_brand":"nike","shoe_name":"air_griffey"}]
$ parquet-tools cat --sample-ratio 0.34 testdata/good.parquet
[]
$ parquet-tools cat --sample-ratio 0.34 testdata/good.parquet
[{"shoe_brand":"steph_curry","shoe_name":"curry7"}]
$ parquet-tools cat --sample-ratio 0.34 testdata/good.parquet
[{"shoe_brand":"nike","shoe_name":"air_griffey"},{"shoe_brand":"fila","shoe_name":"grant_hill_2"}]
$ parquet-tools cat --sample-ratio 0.34 testdata/good.parquet
[{"shoe_brand":"fila","shoe_name":"grant_hill_2"}]
$ parquet-tools cat --sample-ratio 1.0 testdata/good.parquet
[{"shoe_brand":"nike","shoe_name":"air_griffey"},{"shoe_brand":"fila","shoe_name":"grant_hill_2"},{"shoe_brand":"steph_curry","shoe_name":"curry7"}]
$ parquet-tools cat --sample-ratio 0.0 testdata/good.parquet
[]
```

#### Compound Rule

`--skip`, `--limit` and `--sample-ratio` can be used together to achieve certain goals, for example, to get the 3rd row from the parquet file:

```bash
$ parquet-tools cat --skip 2 --limit 1 testdata/good.parquet
[{"shoe_brand":"steph_curry","shoe_name":"curry7"}]
```

The implementation of compound rule is:
* skip rows first, without taking sample ratio into consideration
* output rows based on sample ratio by using random number generator
* once output reaches limit, stop

#### Output Format

> [!CAUTION]
> Since almost all JSON libraries load full JSON into memory to parse and process, it will lead to memory pressure if you dump a huge amount of data. `cat` uses a hack to output data row by row so it will not hit this problem, but downstream facilities may not be able to handle the output.

```bash
$ parquet-tools cat testdata/good.parquet
[{"shoe_brand":"nike","shoe_name":"air_griffey"},{"shoe_brand":"fila","shoe_name":"grant_hill_2"},{"shoe_brand":"steph_curry","shoe_name":"curry7"}]
```

`cat` also supports [line delimited JSON streaming format](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON_2) by specifying `--format jsonl`, allows readers of the output to process in a streaming manner, which will greatly reduce the memory footprint. Note that there is always a newline by end of the output.

> [!TIP]
> If you want to filter data, use JSONL format output and pipe to `jq`.

```bash
$ parquet-tools cat --format jsonl testdata/good.parquet
{"shoe_brand":"nike","shoe_name":"air_griffey"}
{"shoe_brand":"fila","shoe_name":"grant_hill_2"}
{"shoe_brand":"steph_curry","shoe_name":"curry7"}
```

You can read data line by line and parse every single line as a JSON object if you do not have a toolchain to process JSONL format.

If you do not care about order of records, you can use `--concurrent` which will launch multiple encoders (up to number of CPUs) to boost output speed, but does not maintain original order from the parquet file.

```bash
$ parquet-tools cat -f jsonl --concurrent testdata/good.parquet
{"shoe_brand":"fila","shoe_name":"grant_hill_2"}
{"shoe_brand":"nike","shoe_name":"air_griffey"}
{"shoe_brand":"steph_curry","shoe_name":"curry7"}
$ parquet-tools cat -f jsonl --concurrent testdata/good.parquet
{"shoe_brand":"nike","shoe_name":"air_griffey"}
{"shoe_brand":"fila","shoe_name":"grant_hill_2"}
{"shoe_brand":"steph_curry","shoe_name":"curry7"}
```

### import Command

`import` command creates a parquet file based on data in other formats. The target file can be on local file system or cloud storage object like S3, you need to have permission to write to target location. Existing file or cloud storage object will be overwritten.

The command takes 3 parameters, `--source` tells which file (file system only) to load source data, `--format` tells the format of the source data file, it can be `json`, `jsonl` or `csv`, `--schema` points to the file that holds schema. Optionally, you can use `--compression` to specify compression codec, default is "SNAPPY", see [Compression Codecs](#compression-codecs) for available options. You can also use `--data-page-version` to specify the data page format version, see [Data Page Version](#data-page-version) for details. If CSV file contains a header line, you can use `--skip-header` to skip the first line of CSV file.

Each source data file format has its own dedicated schema format:

* CSV: you can refer to [sample in this repo](https://github.com/hangxie/parquet-tools/blob/main/testdata/csv.schema).
* JSON: you can refer to [sample in this repo](https://github.com/hangxie/parquet-tools/blob/main/testdata/json.schema).
* JSONL: use the same schema as JSON format.

Values in CSV and JSON/JSONL are expected to be human-readable format, same as cat command's output, following their converted or logical types:

| Type                               | Format                | Examples                               |
| ---------------------------------- | --------------------- | -------------------------------------- |
| DATE                               | YYYY-MM-DD            | "2024-01-15"                           |
| TIME (MILLIS/MICROS/NANOS)         | HH:MM:SS.nnnnnnnnn    | "10:30:45.123456789"                   |
| TIMESTAMP (MILLIS/MICROS/NANOS)    | RFC3339Nano           | "2024-01-15T10:30:00.123456789Z"       |
| INT96                              | RFC3339Nano           | "2024-01-15T10:30:00.123456789Z"       |
| INTERVAL                           | X mon Y day Z.zzz sec | "2 mon 15 day 7200.000 sec"            |
| INT (8/16/32/64, signed/unsigned)  | Integer value         | 42, -128, 65535                        |
| FLOAT / FLOAT16 / DOUBLE / DECIMAL | Float value           | 3.14, 2.718281828                      |
| UTF8 / STRING / ENUM               | Plain string          | "hello world"                          |
| UUID                               | Standard UUID         | "550e8400-e29b-41d4-a716-446655440000" |
| BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY  | Base64 encoded        | "SGVsbG8gV29ybGQ="                     |
| BOOLEAN                            | true / false          | true, false                            |
| VARIANT                            | JSON object           | {"foo": "bar", "baz": 42}              |

> [!NOTE]
> For `VARIANT` type, see [Variant Data Type Support](#variant-data-type-support) for more details on the required structure.

#### Import from CSV

```bash
$ parquet-tools import -f csv -s testdata/csv.source -m testdata/csv.schema /tmp/csv.parquet
$ parquet-tools row-count /tmp/csv.parquet
10
```

#### Import from JSON

```bash
$ parquet-tools import -f json -s testdata/json.source -m testdata/json.schema -z GZIP /tmp/json.parquet
$ parquet-tools row-count /tmp/json.parquet
1
```

> [!TIP]
> JSON format allows only a single record to be imported, if you want to import multiple records, use JSONL as source format.

#### Import from JSONL

JSONL is [line-delimited JSON streaming format](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON), use JSONL if you want to load multiple JSON objects into parquet.

```bash
$ parquet-tools import -f jsonl -s testdata/jsonl.source -m testdata/jsonl.schema /tmp/jsonl.parquet
$ parquet-tools row-count /tmp/jsonl.parquet
10
```

### inspect Command

`inspect` command provides detailed internal structure inspection of Parquet files at four different levels: file, row group, column chunk, and page. This is useful for debugging, understanding file organization, and analyzing storage efficiency. All output is in JSON format for easy parsing.

The inspect command has hierarchical levels:
1. **File Level** - Overview of the entire file (default)
2. **Row Group Level** - Details of a specific row group (use `--row-group`)
3. **Column Chunk Level** - Details of a column within a row group (use `--row-group` and `--column-chunk`)
4. **Page Level** - Details and actual values from a specific page (use `--row-group`, `--column-chunk`, and `--page`)

#### Inspect File Level

File level inspection shows basic metadata about the parquet file including version, number of row groups, total rows, and compression information:

```bash
$ parquet-tools inspect testdata/good.parquet
{"fileInfo":{"compressedSize":588,"createdBy":"github.com/hangxie/parquet-go v2 latest","numRowGroups":1,"totalRows":3,"uncompressedSize":438,"version":2},"rowGroups":[{"compressedSize":588,"index":0,"numColumns":2,"numRows":3,"totalByteSize":438,"uncompressedSize":438}]}
```

#### Inspect Row Group Level

Row group level inspection shows details of all column chunks within a specific row group, including encodings, compression ratios, and statistics:

```bash
$ parquet-tools inspect testdata/good.parquet --row-group 0
{"columnChunks":[{"compressedSize":269,"compressionCodec":"GZIP","convertedType":"convertedtype=UTF8","encodings":["PLAIN","RLE"],"index":0,"logicalType":"logicaltype=STRING","numValues":3,"pathInSchema":["shoe_brand"],"statistics":{"maxValue":"steph_curry","minValue":"fila","nullCount":0},"type":"BYTE_ARRAY","uncompressedSize":194},{"compressedSize":319,"compressionCodec":"GZIP","convertedType":"convertedtype=UTF8","encodings":["PLAIN","RLE"],"index":1,"logicalType":"logicaltype=STRING","numValues":3,"pathInSchema":["shoe_name"],"statistics":{"maxValue":"grant_hill_2","minValue":"air_griffey","nullCount":0},"type":"BYTE_ARRAY","uncompressedSize":244}],"rowGroup":{"index":0,"numColumns":2,"numRows":3,"totalByteSize":438}}
```

#### Inspect Column Chunk Level

Column chunk level inspection shows all pages within a column chunk, including page types, sizes, and encodings:

```bash
$ parquet-tools inspect testdata/good.parquet --row-group 0 --column-chunk 0
{"columnChunk":{"columnChunkIndex":0,"compressedSize":269,"compressionCodec":"GZIP","convertedType":"convertedtype=UTF8","dataPageOffset":4,"encodings":["PLAIN","RLE"],"logicalType":"logicaltype=STRING","numValues":3,"pathInSchema":["shoe_brand"],"rowGroupIndex":0,"statistics":{"maxValue":"steph_curry","minValue":"fila","nullCount":0},"type":"BYTE_ARRAY","uncompressedSize":194},"pages":[{"index":0,"offset":4,"type":"DATA_PAGE","compressedSize":33,"uncompressedSize":8,"numValues":1,"encoding":"PLAIN","definitionLevelEncoding":"RLE","repetitionLevelEncoding":"RLE","statistics":{"maxValue":"nike","minValue":"nike","nullCount":0}},{"index":1,"offset":82,"type":"DATA_PAGE","compressedSize":33,"uncompressedSize":8,"numValues":1,"encoding":"PLAIN","definitionLevelEncoding":"RLE","repetitionLevelEncoding":"RLE","statistics":{"maxValue":"fila","minValue":"fila","nullCount":0}},{"index":2,"offset":160,"type":"DATA_PAGE","compressedSize":40,"uncompressedSize":15,"numValues":1,"encoding":"PLAIN","definitionLevelEncoding":"RLE","repetitionLevelEncoding":"RLE","statistics":{"maxValue":"steph_curry","minValue":"steph_curry","nullCount":0}}]}
```

For column chunks with dictionary encoding, you'll also see dictionary page information:

```bash
$ parquet-tools inspect testdata/dict-page.parquet --row-group 0 --column-chunk 0
{"columnChunk":{"columnChunkIndex":0,"compressedSize":320,"compressionCodec":"GZIP","convertedType":"convertedtype=UTF8","dataPageOffset":70,"dictionaryPageOffset":4,"encodings":["PLAIN","RLE","RLE_DICTIONARY"],"logicalType":"logicaltype=STRING","numValues":5,"pathInSchema":["shoe_brand"],"rowGroupIndex":0,"statistics":{"maxValue":"reebok","minValue":"adidas","nullCount":0},"type":"BYTE_ARRAY","uncompressedSize":220},"pages":[{"index":0,"offset":4,"type":"DICTIONARY_PAGE","compressedSize":53,"uncompressedSize":28,"numValues":3,"encoding":"PLAIN"},{"index":1,"offset":70,"type":"DATA_PAGE","compressedSize":36,"uncompressedSize":11,"numValues":2,"encoding":"RLE_DICTIONARY","definitionLevelEncoding":"RLE","repetitionLevelEncoding":"RLE","statistics":{"maxValue":"nike","minValue":"adidas","nullCount":0}},{"index":2,"offset":155,"type":"DATA_PAGE","compressedSize":36,"uncompressedSize":11,"numValues":2,"encoding":"RLE_DICTIONARY","definitionLevelEncoding":"RLE","repetitionLevelEncoding":"RLE","statistics":{"maxValue":"reebok","minValue":"nike","nullCount":0}},{"index":3,"offset":240,"type":"DATA_PAGE","compressedSize":31,"uncompressedSize":6,"numValues":1,"encoding":"RLE_DICTIONARY","definitionLevelEncoding":"RLE","repetitionLevelEncoding":"RLE","statistics":{"maxValue":"adidas","minValue":"adidas","nullCount":0}}]}
```

#### Inspect Page Level

Page level inspection shows the actual decoded values from a specific page. This is the most detailed level and is useful for debugging data issues:

```bash
$ parquet-tools inspect testdata/good.parquet --row-group 0 --column-chunk 0 --page 0
{"page":{"index":0,"offset":4,"type":"DATA_PAGE","compressedSize":33,"uncompressedSize":8,"numValues":1,"encoding":"PLAIN","definitionLevelEncoding":"RLE","repetitionLevelEncoding":"RLE","statistics":{"maxValue":"nike","minValue":"nike","nullCount":0}},"values":["nike"]}
```

For dictionary pages, the values show the dictionary entries:

```bash
$ parquet-tools inspect testdata/dict-page.parquet --row-group 0 --column-chunk 0 --page 0
{"page":{"index":0,"offset":4,"type":"DICTIONARY_PAGE","compressedSize":53,"uncompressedSize":28,"numValues":3,"encoding":"PLAIN"},"values":["nike","adidas","reebok"]}
```

> [!TIP]
> Use `inspect` to:
> - Debug why a file is larger than expected
> - Understand compression efficiency
> - Verify dictionary encoding is being used
> - Examine statistics for query optimization
> - Debug data type issues
> - Analyze page sizes and organization

> [!NOTE]
> The `inspect` command is read-only and downloads only the necessary data from remote locations (S3, GCS, etc.), similar to other read commands.

### merge Command

`merge` command merges multiple parquet files into one parquet file, source parquet files need to have the same schema, except top level node can have different names. All source files and target file can be from and to different storage locations.

```bash
$ parquet-tools merge -s testdata/good.parquet,testdata/good.parquet /tmp/doubled.parquet
$ parquet-tools cat -f jsonl testdata/good.parquet
{"shoe_brand":"nike","shoe_name":"air_griffey"}
{"shoe_brand":"fila","shoe_name":"grant_hill_2"}
{"shoe_brand":"steph_curry","shoe_name":"curry7"}
$ parquet-tools cat -f jsonl /tmp/doubled.parquet
{"shoe_brand":"nike","shoe_name":"air_griffey"}
{"shoe_brand":"nike","shoe_name":"air_griffey"}
{"shoe_brand":"fila","shoe_name":"grant_hill_2"}
{"shoe_brand":"steph_curry","shoe_name":"curry7"}
{"shoe_brand":"fila","shoe_name":"grant_hill_2"}
{"shoe_brand":"steph_curry","shoe_name":"curry7"}

$ parquet-tools merge -s testdata/top-level-tag1.parquet -s testdata/top-level-tag2.parquet /tmp/merged.parquet
$ parquet-tools row-count /tmp/merged.parquet
6
```

`--read-page-size` configures how many rows will be read from source file and write to target file each time, you can also use `--compression` to specify compression codec for target parquet file, default is "SNAPPY", see [Compression Codecs](#compression-codecs) for available options. You can use `--data-page-version` to specify the data page format version, see [Data Page Version](#data-page-version) for details. Other read options like `--http-multiple-connection`, `--http-ignore-tls-error`, `--http-extra-headers`, `--object-version`, and `--anonymous` can still be used, but since they are applied to all source files, some of them may not make sense, eg `--object-version`.

When `--concurrent` option is specified, the merge command will read input files in parallel (up to number of CPUs), this can bring performance gain between 5% and 10%, trade-off is that the order of records in the result parquet file will not be strictly in the order of input files.

You can set `--fail-on-int96` option to fail `merge` command for parquet files that contain fields with INT96 type, which is [deprecated](https://issues.apache.org/jira/browse/PARQUET-323), default value for this option is `false` so you can still read INT96 type, but this behavior may change in the future.


### meta Command

`meta` command shows meta data of every row group in a parquet file.

> [!TIP]
> `PathInSchema` uses field name from parquet file, same as `cat` command.

Use `--skip-page-encoding` to skip reading page encoding information. This can speed up the command for remote files as it avoids reading page headers for each column.

#### Show Meta Data

```bash
$ parquet-tools meta testdata/good.parquet
{"NumRowGroups":1,"RowGroups":[{"NumRows":3,"TotalByteSize":438,"Columns":[{"PathInSchema":["shoe_brand"],"Type":"BYTE_ARRAY","ConvertedType":"convertedtype=UTF8","LogicalType":"logicaltype=STRING","Encodings":["PLAIN","RLE"],"CompressedSize":269,"UncompressedSize":194,"NumValues":3,"NullCount":0,"MaxValue":"steph_curry","MinValue":"fila","CompressionCodec":"GZIP"},{"PathInSchema":["shoe_name"],"Type":"BYTE_ARRAY","ConvertedType":"convertedtype=UTF8","LogicalType":"logicaltype=STRING","Encodings":["PLAIN","RLE"],"CompressedSize":319,"UncompressedSize":244,"NumValues":3,"NullCount":0,"MaxValue":"grant_hill_2","MinValue":"air_griffey","CompressionCodec":"GZIP"}]}]}
```

> [!NOTE]
> MinValue, MaxValue and NullCount are optional, if they do not show up in output then it means parquet file does not have that section.

You can set `--fail-on-int96` option to fail `meta` command for parquet files that contain fields with INT96 type, which is [deprecated](https://issues.apache.org/jira/browse/PARQUET-323), default value for this option is `false` so you can still read INT96 type, but this behavior may change in the future.

```bash
$ parquet-tools meta testdata/int96-nil-min-max.parquet
{"NumRowGroups":1,"RowGroups":[{"NumRows":10,"TotalByteSize":488,"Columns":[{"PathInSchema":["Utf8"],"Type":"BYTE_ARRAY","ConvertedType":"convertedtype=UTF8","LogicalType":"logicaltype=STRING","Encodings":["PLAIN","RLE","RLE_DICTIONARY"],"CompressedSize":381,"UncompressedSize":380,"NumValues":10,"NullCount":0,"MaxValue":"UTF8-9","MinValue":"UTF8-0","CompressionCodec":"ZSTD"},{"PathInSchema":["Int96"],"Type":"INT96","Encodings":["PLAIN","RLE"],"CompressedSize":160,"UncompressedSize":108,"NumValues":10,"NullCount":10,"CompressionCodec":"ZSTD"}]}]}
$ parquet-tools meta --fail-on-int96 testdata/int96-nil-min-max.parquet
parquet-tools: error: field Int96 has type INT96 which is not supported
```

### retype Command

`retype` command changes the data type of columns in a parquet file. It supports several type conversions to improve compatibility with tools that don't support certain Parquet types.

**Supported conversions:**
* `--int96-to-timestamp` - Convert INT96 columns to INT64 with TIMESTAMP_NANOS logical type
* `--bson-to-string` - Convert BSON columns to plain strings (JSON encoded)
* `--json-to-string` - Remove JSON logical type from columns (keep as plain BYTE_ARRAY)
* `--float16-to-float32` - Convert FLOAT16 columns to FLOAT32

> [!NOTE]
> These options convert all matching fields in the parquet file; there is currently no way to select particular fields for conversion.

#### Convert INT96 to Timestamp

INT96 is a deprecated timestamp format so lots of tools do not support it, you can use `--int96-to-timestamp` to convert all INT96 columns to INT64 columns with TIMESTAMP (NANOS) logical type.

Following example shows how to retype INT96 to TIMESTAMP so Apache parquet-cli can read the file:

```bash
$ parquet cat testdata/int96-nil-min-max.parquet
Argument error: INT96 is deprecated. As interim enable READ_INT96_AS_FIXED flag to read as byte array.
$ parquet-tools retype --int96-to-timestamp -s testdata/int96-nil-min-max.parquet /tmp/timestamp.parquet
$ parquet cat /tmp/timestamp.parquet
{"Utf8": "UTF8-1", "Int96": null}
{"Utf8": "UTF8-2", "Int96": null}
{"Utf8": "UTF8-3", "Int96": null}
...
```

#### Convert BSON to String

BSON is a binary format that some tools (e.g., DuckDB) do not support. You can use `--bson-to-string` to convert BSON columns to plain strings with STRING logical type. The BSON data is decoded and re-encoded as JSON strings.

```bash
$ parquet-tools retype --bson-to-string -s testdata/all-types.parquet /tmp/bson-to-string.parquet
```

Following example shows how to retype BSON to string so DuckDB can read the file:

```bash
$ duckdb -s 'select count(*) from "testdata/retype.parquet"'
IO Error:
Unsupported converted type (20)
$ parquet-tools retype --bson-to-string -s testdata/retype.parquet /tmp/retype.parquet
$ duckdb -s 'select count(*) from "/tmp/retype.parquet"'
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│      3       │
└──────────────┘
```

#### Remove JSON Logical Type

Some tools may have issues with the JSON logical type annotation. You can use `--json-to-string` to remove the JSON logical type from columns, keeping them as plain BYTE_ARRAY (UTF8 string) without the JSON annotation.

```bash
$ parquet-tools retype --json-to-string -s input.parquet /tmp/json-to-string.parquet
```

#### Convert FLOAT16 to FLOAT32

FLOAT16 (half-precision floating point) is not supported by all tools. You can use `--float16-to-float32` to convert FLOAT16 columns to FLOAT32 (single-precision), which has wider compatibility.

```bash
$ parquet-tools retype --float16-to-float32 -s input.parquet /tmp/float16-to-float32.parquet
```

### row-count Command

`row-count` command provides total number of rows in the parquet file:

#### Show Number of Rows

```bash
$ parquet-tools row-count testdata/good.parquet
3
```

### schema Command

`schema` command shows schema of the parquet file in different formats.

#### JSON Format

JSON format schema can be used directly in parquet-go based golang program like [this example](https://github.com/xitongsys/parquet-go/blob/master/example/json_schema.go):

```bash
$ parquet-tools schema testdata/good.parquet
{"Tag":"name=parquet_go_root, inname=Parquet_go_root","Fields":[{"Tag":"name=shoe_brand, inname=Shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN"},{"Tag":"name=shoe_name, inname=Shoe_name, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN"}]}
```

Schema will output converted type and logical type when they are present in the parquet file, however, default settings will be ignored to make output shorter, e.g.,
* convertedtype=LIST
* convertedtype=MAP
* repetitiontype=REQUIRED
* type=STRUCT

**VARIANT Logical Type:**

The `VARIANT` logical type is generated as `any` in the Go struct format. You can also define your own struct to have more control over compression and encoding. See [Variant Data Type Support](#variant-data-type-support) for more details.

**Encoding Tag:**

The schema command now includes the `encoding` tag which shows the encoding used for each column (e.g., PLAIN, RLE, DELTA_BINARY_PACKED). This information is extracted from the first row group's metadata. Note that parquet files should use consistent encodings for the same column across different row groups - if a file has different encodings for the same column in different row groups, only the first encoding encountered will be shown in the schema.

> [!NOTE]
> If you need to verify encodings for each column across different row groups, use the [`inspect` command](#inspect-command) which provides detailed encoding information at the row group and page level.

Use `--show-compression-codec` to include the `compression` tag in the schema output, showing the compression codec used for each column (e.g., SNAPPY, GZIP, ZSTD). This option works with both JSON and Go struct formats. Default is not to show compression codec so JSON schema and Go struct can be used by codes that utilize old version of parquet-go codes.

```bash
$ parquet-tools schema --show-compression-codec testdata/good.parquet
{"Tag":"name=parquet_go_root, inname=Parquet_go_root","Fields":[{"Tag":"name=shoe_brand, inname=Shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN, compression=GZIP"},{"Tag":"name=shoe_name, inname=Shoe_name, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN, compression=GZIP"}]}

$ parquet-tools schema --format go --show-compression-codec testdata/good.parquet
type Parquet_go_root struct {
	Shoe_brand string `parquet:"name=shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN, compression=GZIP"`
	Shoe_name  string `parquet:"name=shoe_name, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN, compression=GZIP"`
}
```

Use `--skip-page-encoding` to skip reading page encoding information. This can significantly speed up the command for remote files (S3, GCS, HTTP) as it avoids reading page headers. When this flag is set, the `encoding` tag will not be included in the output. This option works with both JSON and Go struct formats.

```bash
$ parquet-tools schema --skip-page-encoding testdata/good.parquet
{"Tag":"name=parquet_go_root, inname=Parquet_go_root","Fields":[{"Tag":"name=shoe_brand, inname=Shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING"},{"Tag":"name=shoe_name, inname=Shoe_name, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING"}]}

$ parquet-tools schema --format go --skip-page-encoding testdata/good.parquet
type Parquet_go_root struct {
	Shoe_brand string `parquet:"name=shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING"`
	Shoe_name  string `parquet:"name=shoe_name, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING"`
}
```

Schema does not output `omitstats` tag as there is no reliable way to determine it.

#### Raw Format

Raw format is the schema directly dumped from parquet file, all other formats are derived from raw format. The `--skip-page-encoding` and `--show-compression-codec` options also apply to raw format output.

```bash
$ parquet-tools schema --format raw testdata/good.parquet
{"repetition_type":"REQUIRED","name":"parquet_go_root","num_children":2,"children":[{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"shoe_brand","converted_type":"UTF8","scale":0,"precision":0,"field_id":0,"logicalType":{"STRING":{}},"encoding":"PLAIN"},{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"shoe_name","converted_type":"UTF8","scale":0,"precision":0,"field_id":0,"logicalType":{"STRING":{}},"encoding":"PLAIN"}]}
```

#### Go Struct Format

go struct format generates go struct definition snippet that can be used in go:

```bash
$ parquet-tools schema --format go testdata/good.parquet
type Parquet_go_root struct {
	Shoe_brand string `parquet:"name=shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN"`
	Shoe_name  string `parquet:"name=shoe_name, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN"`
}
```

You can turn on `--camel-case` to convert field names from snake_case_name to CamelCaseName:

```bash
$ parquet-tools schema --format go --camel-case testdata/good.parquet
type Parquet_go_root struct {
	ShoeBrand string `parquet:"name=shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN"`
	ShoeName  string `parquet:"name=shoe_name, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN"`
}
```

> [!IMPORTANT]
> parquet-go does not support composite type as map key or value in go struct tag as for now so `parquet-tools` will report error if there is such a field, you can still output in raw or JSON format:

```bash
$ parquet-tools schema -f go testdata/map-composite-value.parquet
parquet-tools: error: go struct does not support LIST as MAP value in Parquet_go_root.Scores

$ parquet-tools schema testdata/map-composite-value.parquet
{"Tag":"name=parquet_go_root, inname=Parquet_go_root","Fields":[{"Tag":"name=name, inname=Name, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN"},{"Tag":"name=age, inname=Age, type=INT32, encoding=PLAIN"},{"Tag":"name=id, inname=Id, type=INT64, encoding=PLAIN"},{"Tag":"name=weight, inname=Weight, type=FLOAT, encoding=PLAIN"},{"Tag":"name=sex, inname=Sex, type=BOOLEAN, encoding=PLAIN"},{"Tag":"name=classes, inname=Classes, type=LIST","Fields":[{"Tag":"name=element, inname=Element, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN"}]},{"Tag":"name=scores, inname=Scores, type=MAP","Fields":[{"Tag":"name=key, inname=Key, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN"},{"Tag":"name=value, inname=Value, type=LIST","Fields":[{"Tag":"name=element, inname=Element, type=FLOAT, encoding=PLAIN"}]}]},{"Tag":"name=friends, inname=Friends, type=LIST","Fields":[{"Tag":"name=element, inname=Element","Fields":[{"Tag":"name=name, inname=Name, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN"},{"Tag":"name=id, inname=Id, type=INT64, encoding=PLAIN"}]}]},{"Tag":"name=teachers, inname=Teachers, repetitiontype=REPEATED","Fields":[{"Tag":"name=name, inname=Name, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN"},{"Tag":"name=id, inname=Id, type=INT64, encoding=PLAIN"}]}]}
```

#### CSV Format

CSV format is the schema that can be used to import from CSV files:

```bash
$ parquet-tools schema --format csv testdata/csv-good.parquet
name=Id, type=INT64, encoding=PLAIN
name=Name, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN
name=Age, type=INT32, encoding=PLAIN
name=Temperature, type=FLOAT, encoding=PLAIN
name=Vaccinated, type=BOOLEAN, encoding=PLAIN
```

> [!NOTE]
> Since CSV is a flat 2D format, we cannot generate CSV schema for nested or optional columns:

```bash
$ parquet-tools schema -f csv testdata/csv-optional.parquet
parquet-tools: error: CSV does not support optional column
$ parquet-tools schema -f csv testdata/csv-nested.parquet
parquet-tools: error: CSV supports flat schema only
```

### shell-completions Command (Experimental)

`shell-completions` updates shell's rcfile with proper shell completions setting, this is an experimental feature at this moment, only bash is tested.

#### Install Shell Completions

To install shell completions, run:

```bash
$ parquet-tools shell-completions
```

You will not get output if everything runs well, you can check shell's rcfile, for example, `.bash_profile` or `.bashrc` for bash, to see what it added.

This command will return error if the same line is in shell's rcfile already.

#### Uninstall Shell Completions

To uninstall shell completions, run:

```bash
$ parquet-tools shell-completions --uninstall
```

You will not get output if everything runs well, you can check shell's rcfile, for example, `.bash_profile` or `.bashrc` for bash, to see what it removed.

This command will return error if the line does not exist in shell rcfile.

#### Use Shell Completions

Hit `<TAB>` key in command line when you need hint or want to auto complete current option.

### size Command

`size` command provides various size information, it can be raw data (compressed) size, uncompressed data size, or footer (meta data) size.

#### Show Raw Size

```bash
$ parquet-tools size testdata/good.parquet
588
```

#### Show Footer Size in JSON Format

```bash
$ parquet-tools size --query footer --json testdata/good.parquet
{"Footer":335}
```

#### Show All Sizes in JSON Format

```bash
$ parquet-tools size -q all -j testdata/good.parquet
{"Raw":588,"Uncompressed":438,"Footer":335}
```

### split Command

`split` command distributes data in source file into multiple parquet files, number of output files is either `--file-count` parameter, or total number of rows in source file divided by `--record-count` parameter.

Name of output files is determined by `--name-format` and will be used by `fmt.Sprintf`, default value is `result-%06d.parquet` which means output files will be under current directory with name `result-000000.parquet`, `result-000001.parquet`, etc., you can use any of file locations that support write operation, eg S3, or HDFS.

Other useful parameters include:
* `--fail-on-int96` to fail the command if source parquet file contains INT96 fields
* `--compression` to specify compression codec for output files, default is `SNAPPY`, see [Compression Codecs](#compression-codecs) for available options
* `--data-page-version` to specify data page format version, see [Data Page Version](#data-page-version) for details
* `--read-page-size` to tell how many rows will be read per batch from source

#### Name format

Only one verb for integers is allowed, and it has to be variant of `%b`, `%d`, `%o`, `%x`, or `%X`.

```bash
$ parquet-tools split --name-format file-%0.2f.parquet --file-count 3 testdata/good.parquet
parquet-tools: error: invalid name format [file-%0.2f.parquet]: [%0.2f] is not an allowed format verb
$ parquet-tools split --name-format file.parquet --file-count 3 testdata/good.parquet
parquet-tools: error: invalid name format [file.parquet]: lack of usable verb
```

You can specify width and leading zeros:

```bash
$ parquet-tools split --name-format file-%04b.parquet --file-count 3 testdata/all-types.parquet
$ ls file-*
file-0000.parquet file-0001.parquet file-0010.parquet
```

#### Exact number of output files

```bash
$ parquet-tools row-count testdata/all-types.parquet
10
$ parquet-tools split --file-count 3 testdata/all-types.parquet
$ parquet-tools row-count result-000000.parquet
4
$ parquet-tools row-count result-000001.parquet
3
$ parquet-tools row-count result-000002.parquet
3
```

#### Maximum records in a file

```bash
$ parquet-tools row-count testdata/all-types.parquet
10
$ parquet-tools split --record-count 3 --name-format %d.parquet testdata/all-types.parquet
$ parquet-tools row-count 0.parquet
3
$ parquet-tools row-count 1.parquet
3
$ parquet-tools row-count 2.parquet
3
$ parquet-tools row-count 3.parquet
1
```

### transcode Command

`transcode` command converts a Parquet file to a new Parquet file with the same data but different encoding settings. This is useful for changing compression algorithms, optimizing file size, upgrading page formats, controlling statistics, or preparing files for systems with specific requirements.

The command reads data from a source Parquet file and writes it to a new output file with the specified encoding parameters. All data is preserved exactly - only the storage encoding changes.

#### Change Compression Codec

Use the `--compression` / `-z` parameter to change the compression algorithm. This allows you to optimize file size, improve read/write performance, or ensure compatibility with systems that support specific compression codecs. See [Compression Codecs](#compression-codecs) for the complete list of available options.

Convert a Parquet file from SNAPPY to GZIP compression:

```bash
$ parquet-tools transcode -s testdata/good.parquet -z GZIP /tmp/good-gzip.parquet
$ parquet-tools row-count /tmp/good-gzip.parquet
3
```

Convert to ZSTD for better compression:

```bash
$ parquet-tools transcode -s testdata/all-types.parquet -z ZSTD /tmp/all-types-zstd.parquet
```

Create an uncompressed version for debugging:

```bash
$ parquet-tools transcode -s input.parquet -z UNCOMPRESSED output.parquet
```

#### Change Data Page Version

Use `--data-page-version` to change the data page format. See [Data Page Version](#data-page-version) for details.

```bash
$ parquet-tools transcode -s legacy.parquet --data-page-version=1 compatible.parquet
```

#### Field-Specific Encoding

Use the `--field-encoding` parameter to apply different encodings to specific fields. This allows fine-grained control over encoding on a per-field basis, which is useful when different columns have different data characteristics.

The format is `field.path=ENCODING`, where `field.path` is the dot-separated path to the field. For nested structures, use the full path including intermediate elements like `list` for LIST types and `key_value` for MAP types.

Apply different encodings to different fields:

```bash
$ parquet-tools transcode -s input.parquet \
  --field-encoding "name=DELTA_BYTE_ARRAY" \
  --field-encoding "age=DELTA_BINARY_PACKED" \
  output.parquet
```

For nested fields, use the full path:

```bash
# For a LIST field named "classes" with string elements
$ parquet-tools transcode -s input.parquet \
  --field-encoding "classes.list.element=DELTA_BYTE_ARRAY" \
  output.parquet

# For nested struct fields
$ parquet-tools transcode -s input.parquet \
  --field-encoding "teachers.name=DELTA_BYTE_ARRAY" \
  --field-encoding "friends.list.element.id=DELTA_BINARY_PACKED" \
  output.parquet
```

**Path format for complex types:**
* **Simple fields**: `fieldname` (e.g., `name`, `age`)
* **Nested structs**: `parent.child` (e.g., `teachers.name`)
* **LIST elements**: `fieldname.list.element` (e.g., `classes.list.element`)
* **LIST of structs**: `fieldname.list.element.child` (e.g., `friends.list.element.id`)
* **MAP keys/values**: `fieldname.key_value.key` or `fieldname.key_value.value`

See [Encoding](#encoding) for supported encodings and compatible types.

> [!TIP]
> Use `parquet-tools schema` to see the field structure and determine the correct paths.

#### Control Statistics

Use the `--omit-stats` parameter to control whether column statistics are included in the output file. Statistics enable query engines to skip reading irrelevant data (predicate pushdown), but omitting them reduces file size and write overhead.

Omit statistics for smaller files:

```bash
$ parquet-tools transcode -s input.parquet --omit-stats true output.parquet
```

Ensure statistics are included:

```bash
$ parquet-tools transcode -s input.parquet --omit-stats false output.parquet
```

**Options:**
* `true` - Omit statistics from column chunks (smaller file size, less metadata, faster writes)
* `false` - Include statistics (enables predicate pushdown for query optimization)
* (empty) - Keep original statistics setting from source file

#### Field-Specific Compression

Use the `--field-compression` parameter to apply different compression codecs to specific fields. This allows fine-grained control over compression on a per-field basis, which is useful when different columns have different data characteristics or size requirements.

The format is `field.path=CODEC`, where `field.path` is the dot-separated path to the field (same format as `--field-encoding`).

Apply different compression codecs to different fields:

```bash
$ parquet-tools transcode -s testdata/good.parquet --field-compression shoe_brand=ZSTD --field-compression shoe_name=GZIP /tmp/field-compression.parquet
$ parquet-tools schema --show-compression-codec /tmp/field-compression.parquet
{"Tag":"name=parquet_go_root, inname=Parquet_go_root","Fields":[{"Tag":"name=shoe_brand, inname=Shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN, compression=ZSTD"},{"Tag":"name=shoe_name, inname=Shoe_name, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN, compression=GZIP"}]}
```

For nested fields, use the full path. For example, to set compression for elements in a LIST field:

```bash
$ parquet-tools transcode -s testdata/map-composite-value.parquet --field-compression classes.list.element=ZSTD /tmp/field-compression-list.parquet
```

For nested struct fields (LIST of structs), specify the full path including intermediate elements:

```bash
$ parquet-tools transcode -s testdata/map-composite-value.parquet --field-compression friends.list.element.name=ZSTD --field-compression friends.list.element.id=BROTLI /tmp/field-compression-nested.parquet
```

When both file-level (`-z`/`--compression`) and field-level (`--field-compression`) compression are specified, field-level takes precedence for the specified fields, while file-level is used as the default for other fields:

```bash
$ parquet-tools transcode -s testdata/good.parquet --field-compression shoe_brand=ZSTD -z SNAPPY /tmp/mixed-compression.parquet
$ parquet-tools schema --show-compression-codec /tmp/mixed-compression.parquet
{"Tag":"name=parquet_go_root, inname=Parquet_go_root","Fields":[{"Tag":"name=shoe_brand, inname=Shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN, compression=ZSTD"},{"Tag":"name=shoe_name, inname=Shoe_name, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN, compression=SNAPPY"}]}
```

**Supported compression codecs:**
* `UNCOMPRESSED` - No compression (fastest read/write, largest file size)
* `SNAPPY` - Fast compression with reasonable ratio (default)
* `GZIP` - Good compression ratio, slower than SNAPPY
* `LZ4` - Very fast compression
* `LZ4_RAW` - LZ4 without frame format
* `ZSTD` - Excellent compression ratio with good speed
* `BROTLI` - High compression ratio, slower compression speed

See [Compression Codecs](#compression-codecs) for more details.

> [!TIP]
> Use `parquet-tools schema --show-compression-codec` to see the current compression codec for each field.

#### Combine Multiple Options

You can combine multiple transcode options in a single command:

```bash
$ parquet-tools transcode -s testdata/good.parquet --data-page-version=2 --field-encoding shoe_brand=DELTA_BYTE_ARRAY --field-compression shoe_brand=ZSTD --omit-stats false -z SNAPPY /tmp/transcode-combined.parquet
$ parquet-tools schema --show-compression-codec /tmp/transcode-combined.parquet
{"Tag":"name=parquet_go_root, inname=Parquet_go_root","Fields":[{"Tag":"name=shoe_brand, inname=Shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=DELTA_BYTE_ARRAY, compression=ZSTD"},{"Tag":"name=shoe_name, inname=Shoe_name, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=STRING, encoding=PLAIN, compression=SNAPPY"}]}
```

This example upgrades the page format to v2, sets DELTA_BYTE_ARRAY encoding and ZSTD compression for the "shoe_brand" field, ensures statistics are included, and uses SNAPPY as the default compression for other fields.

#### INT96 Field Detection

Use the `--fail-on-int96` flag to detect and reject files containing INT96 fields.

INT96 is a [deprecated timestamp format](https://issues.apache.org/jira/browse/PARQUET-323) in Parquet. While `transcode` can process files with INT96 fields by default (without converting them), you can use this flag to detect them early in data pipelines.

Detect INT96 fields and fail:

```bash
$ parquet-tools transcode -s testdata/all-types.parquet --fail-on-int96 output.parquet
parquet-tools: error: field Int96 has type INT96 which is not supported
```

Process INT96 files normally (default behavior):

```bash
$ parquet-tools transcode -s testdata/all-types.parquet -z ZSTD output.parquet
# Succeeds - INT96 fields are preserved as-is
```

**Use cases:**
* Data validation pipelines - ensure no deprecated INT96 types exist
* Production systems - fail fast when encountering unsupported types
* Default behavior (flag not set) - INT96 fields are transcoded without modification

### version Command

`version` command provides version, build time, git hash, and source of the executable, it will be quite helpful when you are troubleshooting a problem from this tool itself. Source of the executable can be "source" (or "") which means it was built from source code, or "github" indicates it was from github release (include container images and deb/rpm packages as they share the same build result), or "Homebrew" if it was from homebrew bottles.

#### Print Version

```bash
$ parquet-tools version
v1.45.0
```

#### Print All Information

`-a` is equivalent to `-bs`.

```bash
$ parquet-tools version -a
v1.45.0
2025-12-14T20:17:50Z
Homebrew
```

#### Print Version and Build Time in JSON Format

```bash
$ parquet-tools version --build-time --json
{"Version":"v1.45.0","BuildTime":"2025-12-14T20:17:50Z"}
```

#### Print Version in JSON Format

```bash
$ parquet-tools version -j
{"Version":"v1.45.0"}
```

## Credit

This project is inspired by:

* parquet-go/parquet-tools: https://github.com/xitongsys/parquet-go/tree/master/tool/parquet-tools/
* Python parquet-tools: https://pypi.org/project/parquet-tools/
* Java parquet-tools: https://mvnrepository.com/artifact/org.apache.parquet/parquet-tools
* Makefile: https://github.com/cisco-sso/kdk/blob/master/Makefile

Some test cases are from:

* https://registry.opendata.aws/binding-db/
* https://github.com/xitongsys/parquet-go/tree/master/example/
* https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet
* https://azure.microsoft.com/en-us/services/open-datasets/catalog/
* https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
* https://pro.dp.la/developers/bulk-download
* https://exchange.aboutamazon.com/data-initiative
* https://github.com/apache/parquet-testing/

## License

This project is licensed under the [BSD 3-Clause License](LICENSE).
