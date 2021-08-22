# Installation and Usage of parquet-tools

## Table of Contents

- [Installation](#installation)
  - [Install from Source](#install-from-source)
  - [Download Pre-built Binaries](#download-pre-built-binaries)
  - [Brew Install](#brew-install)
  - [Docker](#docker)
  - [Prebuilt Packages](#prebuilt-packages)
- [Usage](#usage)
  - [Obtain Help](#obtain-help)
  - [Parquet File Location](#parquet-file-location)
    - [File System](#file-system)
    - [S3 Bucket](#s3-bucket)
    - [GCS Bucket](#gcs-bucket)
    - [Azure Storage Container](#azure-storage-container)
  - [cat Command](#cat-command)
    - [Full Data Set](#full-data-set)
    - [Limit Number of Rows](#limit-number-of-rows)
    - [Sampling](#sampling)
    - [Filter](#filter)
  - [import Command](#import-command)
    - [Import from CSV](#import-from-csv)
  - [meta Command](#meta-command)
    - [Show Meta Data](#show-meta-data)
    - [Show Meta Data with base64-encoded Values](#show-meta-data-with-base64-encoded-values)
  - [row-count Command](#row-count-command)
    - [Show Number of Rows](#show-number-of-rows)
  - [schema Command](#schema-command)
    - [JSON Format](#json-format)
    - [Raw Format](#raw-format)
  - [size Command](#size-command)
    - [Show Raw Size](#show-raw-size)
    - [Show Footer Size in JSON Format](#show-footer-size-in-json-format)
    - [Show All Sizes in JSON Format](#show-all-sizes-in-json-format)
  - [version Command](#version-command)
    - [Print Version](#print-version)
    - [Print Version and Build Time in JSON Format](#print-version-and-build-time-in-json-format)
    - [Print Version in JSON Format](#print-version-in-json-format)

## Installation

You can choose one of the installation methods from below, the functionality will be mostly the same.

### Install from Source

Good for people who are familiar with [Go](https://golang.org/).

```
go get github.com/hangxie/parquet-tools
```

it will install latest stable version of `parquet-tools` to $GOPATH/bin, if you do not set `GOPATH` environment variable explicitly, then its default value can be obtained by running `go evn GOPATH`, usually it is `go/` directory under your home directory.

`parquet-tools` installed from source will not report proper version and build time, so if you run `parquet-tools version`, it will just give you an empty line, all other functions are not affected.

### Download Pre-built Binaries

Good for people do not want to build and all other installation approach do not work.

Go to [relase page](https://github.com/hangxie/parquet-tools/releases), pick the release and platform you want to run, download the corresponding gz/zip file, extract it to your local disk, make sure the execution bit is set if you are running on Linux or Mac, then run the program.

For Windows 10 on ARM (like Surface Pro X), use either windows-arm64 or windows-386 build, if you are in Windows Insider program, windows-amd64 build should work too.

### Brew Install

Mac user can use [Homebrew](https://brew.sh/) to install, it is not part of core formula yet but you can run:

```
brew uninstall parquet-tools
brew tap hangxie/tap
brew install go-parquet-tools
```

`parquet-tools` installed by brew is a similar tool built by Java, however, it is [deprecated](https://mvnrepository.com/artifact/org.apache.parquet/parquet-tools-deprecated), since both packages install same `parquet-tools` utility so you need to remove one before installing the other one.

Whenever you want to upgrade to latest version which you should:

```
brew upgrade go-parquet-tools
```

### Docker

Docker image is hosted on [Docker Hub](https://hub.docker.com/r/hangxie/parquet-tools), you can pull the image:

```
docker pull hangxie/parquet-tools
```

Current this project builds docker image for amd64, arm64, and arm/v7.

### Prebuilt Packages

RPM and deb package are work in progress.

## Usage

### Obtain Help
`parquet-tools` provides help information through `-h` flag, whenever you are not sure about parmater for a command, just add `-h` to the end of the line then it will give you all available options, for example:

```
Usage: parquet-tools meta <uri>

Prints the metadata.

Arguments:
  <uri>    URI of Parquet file, support s3:// and file://.

Flags:
  -h, --help       Show context-sensitive help.

  -b, --base-64    Encode min/max value.
```

Most commands can output JSON format result which can be processed by utilities like [jq](https://stedolan.github.io/jq/) or [JSON parser online](https://jsonparseronline.com/).

### Parquet File Location

`parquet-tools` can read and write parquet files from these locations:
* file system
* AWS Simple Storage Service (S3) bucket
* Google Cloud Storage (GCS) bucket
* Azure Storage Container

you need to have proper permission on the file you are going to process.

#### File System

For files from file system, you can specify `file://` scheme or just ignore it:

```
$ parquet-tools row-count cmd/testdata/good.parquet
4
$ parquet-tools row-count file://cmd/testdata/good.parquet
4
$ parquet-tools row-count file://./cmd/testdata/good.parquet
4
```

#### S3 Bucket

Use full S3 URL to indicate S3 object location, it starts with `s3://`. You need to make sure you have permission to read or write the S3 object, the easiest way to verify that is using [AWS cli](https://aws.amazon.com/cli/).

```
$ aws sts get-caller-identity
{
    "UserId": "REDACTED",
    "Account": "123456789012",
    "Arn": "arn:aws:iam::123456789012:user/redacted"
}
$ aws s3 ls s3://dpla-provider-export/2021/04/all.parquet/part-00000-471427c6-8097-428d-9703-a751a6572cca-c000.snappy.parquet
2021-04-14 14:04:51 4632482205 part-00000-471427c6-8097-428d-9703-a751a6572cca-c000.snappy.parquet
$ parquet-tools row-count s3://dpla-provider-export/2021/04/all.parquet/part-00000-471427c6-8097-428d-9703-a751a6572cca-c000.snappy.parquet
14145923
```

Thanks to [parquet-go-source](https://github.com/xitongsys/parquet-go-source), `prquet-tools` only load necessary data from S3 bucket, for most cases it is footer only, so it is much more faster than downloading the file from S3 bucket and run  `parquet-tools` on a local file. The S3 object used in above sample is more 4GB, but the `row-count` command only takes several seconds to finish.

#### GCS Bucket

Use full [gsutil](https://cloud.google.com/storage/docs/gsutil) URI to point to GCS object location, it starts with `gs://`. You need to make sure you have permission to read or write to the GSC object, either use application default or GOOGLE_APPLICATION_CREDENTIALS, you can refer to [Google Cloud document](https://cloud.google.com/docs/authentication/production#automatically) for more details.

```
$ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service/account/key.json
$ parquet-tools import -s cmd/testdata/csv.source -m cmd/testdata/csv.schema gs://REDACTED/csv.parquet
$ parquet-tools row-count gs://REDACTED/csv.parquet
7
```

Similar to S3, `parquet-tools` only downloads necessary data from GCS bucket.

#### Azure Storage Container

`parquet-tools` uses the [HDFS URL format](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-use-blob-storage#access-files-from-within-cluster):
* starts with `wasbs://` (`wasb://` is not supported), followed by
* container as user name, followed by
* storage account as host, followed by
* blob name as path

for example:

> wasbs://public@pandemicdatalake/curated/covid-19/bing_covid-19_data/latest/bing_covid-19_data.parquet

means the parquet file is at:
* storage account `pandemicdatalake`
* container `public`
* blob `curated/covid-19/bing_covid-19_data/latest/bing_covid-19_data.parquet`

`parquet-tools` uses `AZURE_STORAGE_ACCESS_KEY` environment varialbe to identity access, if the blob is public accessible, then `AZURE_STORAGE_ACCESS_KEY` needs to be either empty or unset to indicate that anonmous access is expected.

```
$ AZURE_STORAGE_ACCESS_KEY=REDACTED parquet-tools import -s cmd/testdata/csv.source -m cmd/testdata/csv.schema wasbs://parquet-toos@REDACTED.blob.core.windows.net/test/csv.parquet
$ AZURE_STORAGE_ACCESS_KEY=REDACTED parquet-tools row-count wasbs://parquet-toos@REDACTED.blob.core.windows.net/test/csv.parquet
7
$ AZURE_STORAGE_ACCESS_KEY= parquet-tools row-count wasbs://public@pandemicdatalake.blob.core.windows.net/curated/covid-19/bing_covid-19_data/latest/bing_covid-19_data.parquet
1973310
```

Similar to S3 and GCS, `parquet-tools` only downloads necessary data from blob.

### cat Command

`cat` command output data in parquet file in JSON format. Due to most parquet files are rather large, you should use `row-count` command to have a rough idea how many rows are there in the parquet file, then use `--limit`, `--sample-ration` and the experimental `--filter` flags to reduces the output to a certain level, these flags can be used together.

There is a `page-size` parameter that you probably will never touch it, it tells how many rows `parquet-tools` needs to read from the parquet file every time, you can play with it if you hit performance or resource problem.

#### Full Data Set

```
$ parquet-tools cat cmd/testdata/good.parquet
[{"Shoe_brand":"shoe_brand","Shoe_name":"shoe_name"},{"Shoe_brand":"nike","Shoe_name":"air_griffey"},{"Shoe_brand":"fila","Shoe_name":"grant_hill_2"},{"Shoe_brand":"steph_curry","Shoe_name":"curry7"}]
```

#### Limit Number of Rows

`--limit` is similar to LIMIT in SQL, or `head` in Linux shell, `parquet-tools` will stop running after this many rows outputs.
```
$ parquet-tools cat --limit 2 cmd/testdata/good.parquet
[{"Shoe_brand":"shoe_brand","Shoe_name":"shoe_name"},{"Shoe_brand":"nike","Shoe_name":"air_griffey"}]
```

#### Sampling

`--sample-ratio` enables sampling, the ration is a number between 0.0 and 1.0 inclusively. `1.0` means output everything in the parquet file, while `0.0` means nothing. If you want to have 1 rows out of every 10 rows, use `0.1`.

This feature picks rows in parquet file randomly, so only `0.0` and `1.0` will output deterministic result, all other ratio may generate data set less or more than you want.

```
$ parquet-tools cat --sample-ratio 0.25 cmd/testdata/good.parquet
[{"Shoe_brand":"steph_curry","Shoe_name":"curry7"}]
$ parquet-tools cat --sample-ratio 0.25 cmd/testdata/good.parquet
[{"Shoe_brand":"fila","Shoe_name":"grant_hill_2"}]
$ parquet-tools cat --sample-ratio 0.25 cmd/testdata/good.parquet
[{"Shoe_brand":"shoe_brand","Shoe_name":"shoe_name"}]
$ parquet-tools cat --sample-ratio 0.25 cmd/testdata/good.parquet
[{"Shoe_brand":"fila","Shoe_name":"grant_hill_2"}]
$ parquet-tools cat --sample-ratio 0.25 cmd/testdata/good.parquet
[{"Shoe_brand":"nike","Shoe_name":"air_griffey"},{"Shoe_brand":"fila","Shoe_name":"grant_hill_2"},{"Shoe_brand":"steph_curry","Shoe_name":"curry7"}]
$ parquet-tools cat --sample-ratio 0.25 cmd/testdata/good.parquet
[]
$ parquet-tools cat --sample-ratio 1.0 cmd/testdata/good.parquet
[{"Shoe_brand":"shoe_brand","Shoe_name":"shoe_name"},{"Shoe_brand":"nike","Shoe_name":"air_griffey"},{"Shoe_brand":"fila","Shoe_name":"grant_hill_2"},{"Shoe_brand":"steph_curry","Shoe_name":"curry7"}]
$ parquet-tools cat --sample-ratio 0.0 cmd/testdata/good.parquet
[]
```

#### Filter

`--filter` is an experimental feature that tells `parquet-tools` to output rows conditionally, only rows meet condition of the filter expression will be output, the filter expression is as simple as `field <operator> value`.

Field uses `.` to delimit nested fields, so it can be `topLevel` or `topLevel.secondLevel.thirdLevel`, it currenly does not support field names with `.` inside. If the field does not exist in the row then it is true for not equal to, and false for any other comparisons.

Operator can be `==`, `<>`, `>`, `>=`, `<` and `<=`.

Value can be number (float or integer), string, or `nil`/`null`. `parquet-tools` convert numeric value to float to compare so be aware that `==` sometime may not output the result you want whenever you are dealing with high precision values. String value needs to be quoted by `"`, there is no escape function provided. `nil` or `null` represent a NULL value, a NULL value equals to another NULL value, does not equal to a non-NULL value, everything else will be simply `false`.

```
$ parquet-tools cat --filter 'Shoe_brand == "nike"' cmd/testdata/good.parquet
[{"Shoe_brand":"nike","Shoe_name":"air_griffey"}]
$ parquet-tools cat --filter 'Shoe_brand <> "nike"' cmd/testdata/good.parquet
[{"Shoe_brand":"nike","Shoe_name":"air_griffey"}]
$ parquet-tools cat --filter 'Shoe_brand > "nike"' --limit 1 cmd/testdata/good.parquet
[{"Shoe_brand":"shoe_brand","Shoe_name":"shoe_name"}]
$  cat --filter 'Doc.SourceResource.Date.Begin <> null' --limit 1  s3://dpla-provider-export/2021/04/all.parquet/part-00000-471427c6-8097-428d-9703-a751a6572cca-c000.snappy.parquet
[{"Doc":{"Uri":"http://dp.la/api/items/3e542ba8c7e5f711ca9...
```

### import Command

`import` command creates a paruet file based data file in other format, right now only CSV is supported. The target file can be on local file system or S3 bucket, you need to have permission to write to the specific location. Existing file or S3 object will be overwritten.

The command takes 3 parameters, `--source` tells which file (file system only) to load source data, `--format` tells format of the source data file (only `csv` is supported at this moment), `--schema` points to the file holds schema. The schema file follow parquet-go CSV meta data format, you can refer to [sample in this repo](https://github.com/hangxie/parquet-tools/blob/main/cmd/testdata/csv.schema).

#### Import from CSV

```
$ parquet-tools import -s cmd/testdata/csv.source -m cmd/testdata/csv.schema /tmp/csv.parquet
$ parquet-tools row-count /tmp/csv.parquet
7
```

### meta Command

`meta` command shows meta data of every row group in a parquet file, the `--base64` flag tells `parquet-tools` to output base64 encoded MinValue and MaxValue of a column, otherwise those values will be shown as string.

#### Show Meta Data
```
$ parquet-tools meta cmd/testdata/good.parquet
{"NumRowGroups":1,"RowGroups":[{"NumRows":4,"TotalByteSize":349,"Columns":[{"PathInSchema":["Shoe_brand"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":165,"UncompressedSize":161,"NumValues":4,"NullCount":0,"MaxValue":"steph_curry","MinValue":"fila"},{"PathInSchema":["Shoe_name"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":192,"UncompressedSize":188,"NumValues":4,"NullCount":0,"MaxValue":"shoe_name","MinValue":"air_griffey"}]}]}
```

#### Show Meta Data with base64-encoded Values

```
$ parquet-tools meta --base64 cmd/testdata/good.parquet
{"NumRowGroups":1,"RowGroups":[{"NumRows":4,"TotalByteSize":349,"Columns":[{"PathInSchema":["Shoe_brand"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":165,"UncompressedSize":161,"NumValues":4,"NullCount":0,"MaxValue":"c3RlcGhfY3Vycnk=","MinValue":"ZmlsYQ=="},{"PathInSchema":["Shoe_name"],"Type":"BYTE_ARRAY","Encodings":["RLE","BIT_PACKED","PLAIN"],"CompressedSize":192,"UncompressedSize":188,"NumValues":4,"NullCount":0,"MaxValue":"c2hvZV9uYW1l","MinValue":"YWlyX2dyaWZmZXk="}]}]}
```

Note that MinValue, MaxValue and NullCount are optional, if they do not show up in output then it means parquet file does not have that section.

### row-count Command

`row-count` command provides total number of rows in the parquet file:

#### Show Number of Rows

```
$ parquet-tools row-count cmd/testdata/good.parquet
4
```

### schema Command

`schema` command shows schema of the parquet file in differnt formats.

#### JSON Format

JSON format schema can be used directly in parquet-go based golang program like [this example](https://github.com/xitongsys/parquet-go/blob/master/example/json_schema.go):

```
$ parquet-tools schema cmd/testdata/good.parquet
{"Tag":"name=Parquet_go_root, repetitiontype=REQUIRED","Fields":[{"Tag":"name=Shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"},{"Tag":"name=Shoe_name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"}]}
```

#### Raw Format

Raw format is the schema directly dumped from parquet file, all other formats are derived from raw format.

```
$ parquet-tools schema --format raw cmd/testdata/good.parquet
{"repetition_type":"REQUIRED","name":"Parquet_go_root","num_children":2,"children":[{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"Shoe_brand","converted_type":"UTF8","scale":0,"precision":0,"field_id":0,"logicalType":{"STRING":{}}},{"type":"BYTE_ARRAY","type_length":0,"repetition_type":"REQUIRED","name":"Shoe_name","converted_type":"UTF8","scale":0,"precision":0,"field_id":0,"logicalType":{"STRING":{}}}]}
```

### size Command

`size` command provides various size information, it can be raw data (compressed) size, uncompressed data size, or footer (meta data) size.

#### Show Raw Size

```
$ parquet-tools size cmd/testdata/good.parquet
357
```

#### Show Footer Size in JSON Format

```
$ parquet-tools size --query footer --json cmd/testdata/good.parquet
{"Footer":316}
```

#### Show All Sizes in JSON Format

```
$ parquet-tools size -q all -j cmd/testdata/good.parquet
{"Raw":357,"Uncompressed":349,"Footer":316}
```

### version Command

`version` command provides version and build information, it will be quite helpful when you are troubleshooting a problem from this tool itself.

#### Print Version

```
$ parquet-tools version
v1.2.0
```

#### Print Version and Build Time in JSON Format

```
$ parquet-tools version --build-time --json
v1.2.0
2021-05-26T18:13:55-07:00
```

#### Print Version in JSON Format

```
parquet-tools version -j
{"Version":"v1.2.0"}
```
