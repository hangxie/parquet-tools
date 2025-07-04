[![](https://img.shields.io/badge/license-BSD%203-blue)](https://github.com/hangxie/parquet-tools/blob/main/LICENSE)
[![](https://img.shields.io/github/v/tag/hangxie/parquet-tools.svg?color=brightgreen&label=version&sort=semver)](https://github.com/hangxie/parquet-tools/releases)
[![[parquet-tools]](https://github.com/hangxie/parquet-tools/actions/workflows/release.yml/badge.svg)](https://github.com/hangxie/parquet-tools/actions/workflows/release.yml)
[![](https://goreportcard.com/badge/github.com/hangxie/parquet-tools)](https://goreportcard.com/report/github.com/hangxie/parquet-tools)
[![](https://github.com/hangxie/parquet-tools/wiki/coverage.svg)](https://github.com/hangxie/parquet-tools/wiki/Coverage-Report)

# parquet-tools
Utility to inspect Parquet files.

## Quick Start

Pre-built binary or package can be found from [release page](https://github.com/hangxie/parquet-tools/releases), on Mac you can install with brew:

```bash
$ brew install go-parquet-tools
```

Once it is installed

```bash
$ parquet-tools
Usage: parquet-tools <command> [flags]

Utility inspect Parquet files, for full usage see https://github.com/hangxie/parquet-tools/blob/main/README.md

Flags:
  -h, --help    Show context-sensitive help.

Commands:
  cat                  Prints the content of a Parquet file, data only.
  import               Create Parquet file from other source data.
  merge                Merge multiple parquet files into one.
  meta                 Prints the metadata.
  row-count            Prints the count of rows.
  schema               Prints the schema.
  shell-completions    Install/uninstall shell completions
  size                 Prints the size.
  split                Split into multiple parquet files.
  version              Show build version.

Run "parquet-tools <command> --help" for more information on a command.

parquet-tools: error: expected one of "cat", "import", "merge", "meta", "row-count", ...
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
    - [merge Command](#merge-command)
    - [meta Command](#meta-command)
      - [Show Meta Data](#show-meta-data)
      - [Show Meta Data with Base64-encoded Values](#show-meta-data-with-base64-encoded-values)
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

Above command installs latest released version of `parquet-tools` to $GOPATH/bin, `parquet-tools` installed from source will not report proper version and build time, so if you run `parquet-tools version`, it will just give you an empty line, all other functions are not affected.

> [!TIP]
> If you do not set `GOPATH` environment variable explicitly, then its default value can be obtained by running `go env GOPATH`, usually it is `go/` directory under your home directory.

### Download Pre-built Binaries

Good for people do not want to build and all other installation approaches do not work.

Go to [release page](https://github.com/hangxie/parquet-tools/releases), pick the release and platform you want to run, download the corresponding gz/zip file, extract it to your local disk, make sure the execution bit is set if you are running on Linux, Mac, or FreeBSD, then run the program.

For Windows 10 on ARM (like Surface Pro X), use windows-arm64, if you are using Windows 11 on ARM, both windows-arm64 and windows-amd64 build should work.

### Brew Install

Mac user can use [Homebrew](https://brew.sh/) to install:

```bash
$ brew install go-parquet-tools
```

To upgrade to latest version:

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
v1.32.2
$ podman run --rm ghcr.io/hangxie/parquet-tools version
v1.32.2
```

### Prebuilt RPM and deb Packages

RPM and deb package can be found on [release page](https://github.com/hangxie/parquet-tools/releases), only amd64/x86_64 and arm64/aarch64 arch are available at this moment, download the proper package and run corresponding installation command:

* On Debian/Ubuntu:

```bash
$ sudo dpkg -i parquet-tools_1.32.2_amd64.deb
Preparing to unpack parquet-tools_1.32.2_amd64.deb ...
Unpacking parquet-tools (1.32.2) ...
Setting up parquet-tools (1.32.2) ...
```

* On CentOS/Fedora:

```bash
$ sudo rpm -Uhv parquet-tools-1.32.2-1.x86_64.rpm
Verifying...                         ################################# [100%]
Preparing...                         ################################# [100%]
Updating / installing...
   1:parquet-tools-1.32.2-1          ################################# [100%]
```

## Usage

### Obtain Help
`parquet-tools` provides help information through `-h` flag, whenever you are not sure about parameter for a command, just add `-h` to the end of the line then it will give you all available options, for example:

```bash
$ parquet-tools meta -h
Usage: parquet-tools meta <uri> [flags]

Prints the metadata.

Arguments:
  <uri>    URI of Parquet file.

Flags:
  -h, --help                        Show context-sensitive help.

      --http-multiple-connection    (HTTP URI only) use multiple HTTP connection.
      --http-ignore-tls-error       (HTTP URI only) ignore TLS error.
      --http-extra-headers=         (HTTP URI only) extra HTTP headers.
      --object-version=""           (S3, GCS, and Azure only) object version.
      --anonymous                   (S3, GCS, and Azure only) object is publicly accessible.
  -b, --base64                      Encode min/max value.
      --fail-on-int96               fail command if INT96 data type presents.
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
> you need to have proper permission on the file you are going to process.

#### File System

For files from file system, you can specify `file://` scheme or just ignore it:

```bash
$ parquet-tools row-count testdata/good.parquet
4
$ parquet-tools row-count file://testdata/good.parquet
4
$ parquet-tools row-count file://./testdata/good.parquet
4
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

Use full [gsutil](https://cloud.google.com/storage/docs/gsutil) URI to point to GCS object location, it starts with `gs://`. You need to make sure you have permission to read or write to the GSC object, either use application default or GOOGLE_APPLICATION_CREDENTIALS, you can refer to [Google Cloud document](https://cloud.google.com/docs/authentication/production#automatically) for more details.

```bash
$ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service/account/key.json
$ parquet-tools import -s testdata/csv.source -m testdata/csv.schema gs://REDACTED/csv.parquet
$ parquet-tools row-count gs://REDACTED/csv.parquet
7
```

Similar to S3, `parquet-tools` downloads only necessary data from GCS bucket.

If the GCS object is publicly accessible, you can use `--anonymous` option to indicate that anonymous access is expected:

```
$ parquet-tools row-count gs://cloud-samples-data/bigquery/us-states/us-states.parquet
parquet-tools: error: failed to create GCS client: dialing: google: could not find default credentials. See https://cloud.google.com/docs/authentication/external/set-up-adc for more information
$ parquet-tools row-count --anonymous gs://cloud-samples-data/bigquery/us-states/us-states.parquet
50
```

Optionally, you can specify object generation by using `--object-version` when you perform read operation (like cat, row-count, schema, etc.), `parquet-tools` will access latest generation if this parameter is omitted.

```
$ parquet-tools row-count --anonymous gs://cloud-samples-data/bigquery/us-states/us-states.parquet
50
$ parquet-tools row-count --anonymous --object-version=-1 gs://cloud-samples-data/bigquery/us-states/us-states.parquet
50
```

`parquet-tools` reports error on invalid or non-existent generations:

```
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

for example:

> wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet

means the parquet file is at:
* storage account `azureopendatastorage`
* container `laborstatisticscontainer`
* blob `lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet`

`parquet-tools` uses `AZURE_STORAGE_ACCESS_KEY` environment variable to identity access:

```bash
$ AZURE_STORAGE_ACCESS_KEY=REDACTED parquet-tools import -s testdata/csv.source -m testdata/csv.schema wasbs://REDACTED@REDACTED.blob.core.windows.net/test/csv.parquet
$ AZURE_STORAGE_ACCESS_KEY=REDACTED parquet-tools row-count wasbs://REDACTED@REDACTED.blob.core.windows.net/test/csv.parquet
7
```

If the blob is publicly accessible, either unset `AZURE_STORAGE_ACCESS_KEY` or use `--anonymous` option to indicate that anonymous access is expected:

```
$ AZURE_STORAGE_ACCESS_KEY= parquet-tools row-count wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet
6582726
$ parquet-tools row-count --anonymous wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet
6582726
```

Optionally, you can specify object version by using `--object-version` when you perform read operation (like cat, row-count, schema, etc.) for Azure blob, `parquet-tools` will access current version if this parameter is omitted.

> [!NOTE]
> Azure blob returns different errors for non-existent version and invalid version id:
```
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
* `--http-multiple-connection` will enable dedicated transport for concurrent requests, `parquet-tools` will establish multiple TCP connections to remote server. This may or may not have performance impact depends on how remote server handles concurrent connections, it is recommended to leave it to default `false` value for all commands except `cat`, and test performance carefully with `cat` command.
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

```
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

### cat Command

`cat` command outputs data in parquet file, it supports JSON, JSONL, CSV, and TSV format. Due to most parquet files are rather large, you should use `row-count` command to have a rough idea how many rows are there in the parquet file, then use `--skip`, `--limit` and `--sample-ratio` flags to reduces the output to a certain level, these flags can be used together.

There are two parameters that you probably will never touch:

* `--read-page-size` tells how many rows `parquet-tools` needs to read from the parquet file every time, you can play with it if you hit performance or resource problem.

#### Full Data Set

```bash
$ parquet-tools cat --format jsonl testdata/good.parquet
{"shoe_brand":"nike","shoe_name":"air_griffey"}
{"shoe_brand":"fila","shoe_name":"grant_hill_2"}
{"shoe_brand":"steph_curry","shoe_name":"curry7"}
```

> [!TIP]
> You can set `--fail-on-int96` option to fail `cat` command for parquet files contain fields with INT96 type, which is [deprecated](https://issues.apache.org/jira/browse/PARQUET-323), default value for this option is `false` so you can still read INT96 type, but this behavior may change in the future.

```bash
$ parquet-tools cat --fail-on-int96 testdata/all-types.parquet
parquet-tools: error: field Int96 has type INT96 which is not supported
$ parquet-tools cat testdata/all-types.parquet
[{"Bool":true,"ByteArray":"ByteArray-0","Date":1640995200,...
```

#### Skip Rows

`--skip` is similar to OFFSET in SQL, `parquet-tools` will skip this many rows from beginning of the parquet file before applying other logics.

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
> There is no standard for CSV and TSV format, `parquet-tools` utilizes Go's `encoding/csv` module to maximize compatibility, however, there is no guarantee that output can be interpreted by other utilities, especially if they are from other programming languages.

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

`--limit` is similar to LIMIT in SQL, or `head` in Linux shell, `parquet-tools` will stop running after this many rows outputs.

```bash
$ parquet-tools cat --limit 2 testdata/good.parquet
[{"shoe_brand":"nike","shoe_name":"air_griffey"},{"shoe_brand":"fila","shoe_name":"grant_hill_2"}]
```

#### Sampling

`--sample-ratio` enables sampling, the ration is a number between 0.0 and 1.0 inclusively. `1.0` means output everything in the parquet file, while `0.0` means nothing. If you want to have 1 rows out of every 10 rows, use `0.1`.

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
[{"Shoe_brand":"nike","shoe_name":"air_griffey"},{"shoe_brand":"fila","shoe_name":"grant_hill_2"}]
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

#### Output Format

> [!CAUTION]
> `cat` supports two output formats, one is the default JSON format that wraps all JSON objects into a list, this works perfectly with small output and is compatible with most JSON toolchains, however, since almost all JSON libraries load full JSON into memory to parse and process, this will lead to memory pressure if you dump a huge amount of data.

```bash
$ parquet-tools cat testdata/good.parquet
[{"shoe_brand":"nike","shoe_name":"air_griffey"},{"shoe_brand":"fila","shoe_name":"grant_hill_2"},{"shoe_brand":"steph_curry","shoe_name":"curry7"}]
```

`cat` also supports [line delimited JSON streaming format](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON_2) format by specifying `--format jsonl`, allows reader of the output to process in a streaming manner, which will greatly reduce the memory footprint. Note that there is always a newline by end of the output.

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

```
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

`import` command creates a parquet file based from data in other format. The target file can be on local file system or cloud storage object like S3, you need to have permission to write to target location. Existing file or cloud storage object will be overwritten.

The command takes 3 parameters, `--source` tells which file (file system only) to load source data, `--format` tells format of the source data file, it can be `json`, `jsonl` or `csv`, `--schema` points to the file holds schema. Optionally, you can use `--compression` to specify compression codec (UNCOMPRESSED/SNAPPY/GZIP/LZ4/LZ4_RAW/ZSTD), default is "SNAPPY". If CSV file contains a header line, you can use `--skip-header` to skip the first line of CSV file.

Each source data file format has its own dedicated schema format:

* CSV: you can refer to [sample in this repo](https://github.com/hangxie/parquet-tools/blob/main/testdata/csv.schema).
* JSON: you can refer to [sample in this repo](https://github.com/hangxie/parquet-tools/blob/main/testdata/json.schema).
* JSONL: use same schema as JSON format.

> [!WARNING]
> You cannot import INT96 data at this moment, more details can be found at https://github.com/hangxie/parquet-tools/issues/149.

#### Import from CSV

```bash
$ parquet-tools import -f csv -s testdata/csv.source -m testdata/csv.schema /tmp/csv.parquet
$ parquet-tools row-count /tmp/csv.parquet
7
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
7
```

### merge Command

`merge` command merge multiple parquet files into one parquet file, source parquet files need to have same schema, except top level node can have different names. All source files and target file can be from and to different storage locations.

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

`--read-page-size` configures how many rows will be read from source file and write to target file each time, you can also use `--compression` to specify compression codec (UNCOMPRESSED/SNAPPY/GZIP/LZ4/LZ4_RAW/ZSTD) for target parquet file, default is "SNAPPY". Other read options like `--http-multiple-connection`, `--http-ignore-tls-error`, `--http-extra-headers`, `--object-version`, and `--anonymous` can still be used, but since they are applied to all source files, some of them may not make sense, eg `--object-version`.

When `--concurrent` option is specified, the merge command will read input files in parallel (up to number of CPUs), this can bring performance gain between 5% and 10%, trade-off is that the order of records in the result parquet file will not be strictly in the order of input files.

You can set `--fail-on-int96` option to fail `merge` command for parquet files contain fields with INT96 type, which is [deprecated](https://issues.apache.org/jira/browse/PARQUET-323), default value for this option is `false` so you can still read INT96 type, but this behavior may change in the future.


### meta Command

`meta` command shows meta data of every row group in a parquet file.

> [!NOTE]
> `PathInSchema` uses field name from parquet file, same as `cat` command.

#### Show Meta Data

```bash
$ parquet-tools meta testdata/good.parquet
{"NumRowGroups":1,"RowGroups":[{"NumRows":3,"TotalByteSize":438,"Columns":[{"PathInSchema":["shoe_brand"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":269,"UncompressedSize":194,"NumValues":3,"NullCount":0,"MaxValue":"steph_curry","MinValue":"fila","CompressionCodec":"GZIP"},{"PathInSchema":["shoe_name"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":319,"UncompressedSize":244,"NumValues":3,"NullCount":0,"MaxValue":"grant_hill_2","MinValue":"air_griffey","CompressionCodec":"GZIP"}]}]}
```

#### Show Meta Data with Base64-encoded Values

```bash
$ parquet-tools meta --base64 testdata/good.parquet
{"NumRowGroups":1,"RowGroups":[{"NumRows":3,"TotalByteSize":438,"Columns":[{"PathInSchema":["shoe_brand"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":269,"UncompressedSize":194,"NumValues":3,"NullCount":0,"MaxValue":"c3RlcGhfY3Vycnk=","MinValue":"ZmlsYQ==","CompressionCodec":"GZIP"},{"PathInSchema":["shoe_name"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":319,"UncompressedSize":244,"NumValues":3,"NullCount":0,"MaxValue":"Z3JhbnRfaGlsbF8y","MinValue":"YWlyX2dyaWZmZXk=","CompressionCodec":"GZIP"}]}]}
```

> [!NOTE]
> MinValue, MaxValue and NullCount are optional, if they do not show up in output then it means parquet file does not have that section.

You can set `--fail-on-int96` option to fail `meta` command for parquet files contain fields with INT96 type, which is [deprecated](https://issues.apache.org/jira/browse/PARQUET-323), default value for this option is `false` so you can still read INT96 type, but this behavior may change in the future.

```bash
$ parquet-tools meta testdata/int96-nil-min-max.parquet
{"NumRowGroups":1,"RowGroups":[{"NumRows":10,"TotalByteSize":488,"Columns":[{"PathInSchema":["Utf8"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN","PLAIN_DICTIONARY","RLE_DICTIONARY"],"CompressedSize":381,"UncompressedSize":380,"NumValues":10,"NullCount":0,"MaxValue":"UTF8-9","MinValue":"UTF8-0","CompressionCodec":"ZSTD"},{"PathInSchema":["Int96"],"Type":"INT96","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":160,"UncompressedSize":108,"NumValues":10,"NullCount":10,"CompressionCodec":"ZSTD"}]}]}
$ parquet-tools meta --fail-on-int96 testdata/int96-nil-min-max.parquet
parquet-tools: error: field Int96 has type INT96 which is not supported
```

### row-count Command

`row-count` command provides total number of rows in the parquet file:

#### Show Number of Rows

```bash
$ parquet-tools row-count testdata/good.parquet
4
```

### schema Command

`schema` command shows schema of the parquet file in different formats.

#### JSON Format

JSON format schema can be used directly in parquet-go based golang program like [this example](https://github.com/xitongsys/parquet-go/blob/master/example/json_schema.go):

```bash
$ parquet-tools schema testdata/good.parquet
{"Tag":"name=parquet_go_root","Fields":[{"Tag":"name=shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8"},{"Tag":"name=shoe_name, type=BYTE_ARRAY, convertedtype=UTF8"}]}
```

Default setting will be ignored to make output shorter, eg
* convertedtype=LIST
* convertedtype=MAP
* repetitiontype=REQUIRED
* type=STRUCT

#### Raw Format

Raw format is the schema directly dumped from parquet file, all other formats are derived from raw format.

```bash
$ parquet-tools schema --format raw testdata/good.parquet
{"repetition_type":"REQUIRED","name":"parquet_go_root","num_children":2,"children":[{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"shoe_brand","converted_type":"UTF8","scale":0,"precision":0,"field_id":0,"logicalType":{"STRING":{}}},{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"shoe_name","converted_type":"UTF8","scale":0,"precision":0,"field_id":0,"logicalType":{"STRING":{}}}]}
```

#### Go Struct Format

go struct format generate go struct definition snippet that can be used in go:

```bash
$ parquet-tools schema --format go testdata/good.parquet | gofmt
type Parquet_go_root struct {
	Shoe_brand string `parquet:"name=shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8"`
	Shoe_name  string `parquet:"name=shoe_name, type=BYTE_ARRAY, convertedtype=UTF8"`
}
```

Based on your use case, type `Parquet_go_root` may need to be renamed.

> [!IMPORTANT]
> parquet-go does not support composite type as map key or value in go struct tag as for now so `parquet-tools` will report error if there is such a field, you can still output in raw or JSON format:

```bash
$ parquet-tools schema -f go testdata/map-composite-value.parquet
parquet-tools: error: go struct does not support composite type as map value in field [Parquet_go_root.Scores]

$ parquet-tools schema testdata/map-composite-value.parquet
{"Tag":"name=parquet_go_root","Fields":[{"Tag":"name=name, type=BYTE_ARRAY, convertedtype=UTF8"},{"Tag":"name=age, type=INT32"},{"Tag":"name=id, type=INT64"},{"Tag":"name=weight, type=FLOAT"},{"Tag":"name=sex, type=BOOLEAN"},{"Tag":"name=classes, type=LIST","Fields":[{"Tag":"name=element, type=BYTE_ARRAY, convertedtype=UTF8"}]},{"Tag":"name=scores, type=MAP","Fields":[{"Tag":"name=key, type=BYTE_ARRAY, convertedtype=UTF8"},{"Tag":"name=value, type=LIST","Fields":[{"Tag":"name=element, type=FLOAT"}]}]},{"Tag":"name=friends, type=LIST","Fields":[{"Tag":"name=element","Fields":[{"Tag":"name=name, type=BYTE_ARRAY, convertedtype=UTF8"},{"Tag":"name=id, type=INT64"}]}]},{"Tag":"name=teachers, repetitiontype=REPEATED","Fields":[{"Tag":"name=name, type=BYTE_ARRAY, convertedtype=UTF8"},{"Tag":"name=id, type=INT64"}]}]}
```

#### CSV Format

CSV format is the schema that can be used to import from CSV files:

```bash
$ parquet-tools schema --format csv testdata/csv-good.parquet
name=Id, type=INT64
name=Name, type=BYTE_ARRAY, convertedtype=UTF8
name=Age, type=INT32
name=Temperature, type=FLOAT
name=Vaccinated, type=BOOLEAN
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

To install shell completions. run:

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
{"Footer":323}
```

#### Show All Sizes in JSON Format

```bash
$ parquet-tools size -q all -j testdata/good.parquet
{"Raw":588,"Uncompressed":438,"Footer":323}
```

### split Command

`split` command distributes data in source file into multiple parquet files, number of output files is either `--file-count` parameter, or total number of rows in source file divided by `--record-count` parameter.

Name of output files is determined by `--name-format` and will be used by `fmt.Sprintf`, default value is `result-%06d.parquet` which means output files will be under current directory with name `result-000000.parquet`, `result-000001.parquet`, etc., you can use any of file locations that support write operation, eg S3, or HDFS.

Other useful parameters include:
* `--fail-on-int96` to fail the command if source parquet file contains INT96 fields
* `--compression` to specify compression codec for output files, default is `SNAPPY`
* `--read-page-size` to tell how many rows will be read per batch from source

#### Name format

There is only one verb for integer is allowed, and it has to be variant of `%b`, `%d`, `%o`, `%x`, or `%X`.

```bash
$ parquet-tools split --name-format file-%0.2f.parquet --file-count 3 testdata/good.parquet
parquet-tools: error: invalid name format [file-%0.2f.parquet]: [%0.2f] is not an allowed format verb
$ parquet-tools split --name-format file.parquet --file-count 3 testdata/good.parquet
parquet-tools: error: invalid name format [file.parquet]: lack of useable verb
```

You can use specify width and leading zeros:

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

### version Command

`version` command provides version, build time, git hash, and source of the executable, it will be quite helpful when you are troubleshooting a problem from this tool itself. Source of the executable can be "source" (or "") which means it was built from source code, or "github" indicates it was from github release (include container images and deb/rpm packages as they share the same build result), or "Homebrew" if it was from homebrew bottles.

#### Print Version

```bash
$ parquet-tools version
v1.32.2
```

#### Print All Information

`-a` is equivalent to `-bs`.

```bash
$ parquet-tools version -a
v1.32.2
2025-06-07T21:31:01-0700
Makefile
```

#### Print Version and Build Time in JSON Format

```bash
$ parquet-tools version --build-time --json
{"Version":"v1.32.2","BuildTime":"2025-06-07T21:31:01-0700"}
```

#### Print Version in JSON Format

```bash
$ parquet-tools version -j
{"Version":"v1.32.2"}
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

## License

This project is licensed under the [BSD 3-Clause License](LICENSE).
