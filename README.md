# parquet-tools
Utility to deal with Parquet data

## Credit
This project is inspired by:

* Go parquet-tools: https://github.com/xitongsys/parquet-go/tree/master/tool/parquet-tools/
* Python parquet-tools: https://pypi.org/project/parquet-tools/
* Java parquet-tools: https://mvnrepository.com/artifact/org.apache.parquet/parquet-tools
* Makefile: https://github.com/cisco-sso/kdk/blob/master/Makefile

Some test cases are from:

* https://pro.dp.la/developers/bulk-download
* https://github.com/xitongsys/parquet-go/tree/master/example/

Tools used:

* https://golang.org/
* https://github.com/golangci/golangci-lint
* https://github.com/jstemmer/go-junit-report
* https://circleci.com/

## How-To

* Install from source:
   ```
   go get github.com/hangxie/parquet-tools
   ```

* run:
   ```
   parquet-tools -h
   Usage: parquet-tools <command>

   Flags:
     -h, --help    Show context-sensitive help.

   Commands:
     cat          Prints the content of a Parquet file, data only.
     import       Create Parquet file from other source data.
     meta         Prints the metadata.
     row-count    Prints the count of rows.
     schema       Prints the schema.
     size         Prints the size.
     version      Show build version.

   Run "parquet-tools <command> --help" for more information on a command.
   ```

## TODO

TODO list is tracked as enhancement in issues.

CircleCI Build Status: [![parquet-tools](https://circleci.com/gh/hangxie/parquet-tools.svg?style=svg)](https://circleci.com/gh/hangxie/parquet-tools)
