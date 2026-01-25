# Compatibility

This document tracks known compatibility issues between parquet-tools and other Parquet utilities.

## Table of Contents

- [Apache parquet-cli / PySpark](#apache-parquet-cli--pyspark)
- [pqrs](#pqrs)
- [Python parquet-tools](#python-parquet-tools)
- [DuckDB](#duckdb)
- [PyArrow](#pyarrow)
- [pandas](#pandas)
- [Polars](#polars)

## Overview

Parquet files created or modified by parquet-tools should be readable by other tools, and vice versa. This document lists known issues and workarounds.

## Apache parquet-cli / PySpark

[Apache parquet-cli](https://github.com/apache/parquet-java/tree/master/parquet-cli) is the official Java-based CLI from the Apache Parquet project. [PySpark](https://spark.apache.org/docs/latest/api/python/) uses the same underlying Java Parquet library and has the same compatibility issues.

**Known Issues:**

#### 1. INT96 Timestamp Type

parquet-cli has deprecated INT96 and will not read files containing this type by default.

```
Argument error: INT96 is deprecated. As interim enable READ_INT96_AS_FIXED flag to read as byte array.
```

**Workaround:** Use `parquet-tools retype --int96-to-timestamp` to convert INT96 columns to a supported timestamp type before reading with parquet-cli.

#### 2. Brotli Compression Codec

parquet-cli does not support Brotli compression out of the box.

```
Caused by: org.apache.parquet.hadoop.BadConfigurationException: Class org.apache.hadoop.io.compress.BrotliCodec was not found
```

**Workaround:** Use `parquet-tools transcode` to convert Brotli-compressed columns to a supported codec (gzip, snappy, lz4_raw, or uncompressed).

#### 3. UUID Logical Type

parquet-cli does not properly support the UUID logical type. When reading a file with correct UUID schema (`fixed_len_byte_array(16)` with UUID logical type), parquet-cli incorrectly requests it as `binary (STRING)`.

```
Caused by: org.apache.parquet.io.ParquetDecodingException: The requested schema is not compatible with the file schema.
incompatible types: required binary Uuid (STRING) != required fixed_len_byte_array(16) Uuid (UUID)
```

Note: parquet-tools writes UUID in the correct format per the Parquet specification. This is a parquet-cli limitation.

**Workaround:** Use `parquet-tools retype --uuid-to-string` to convert UUID columns to string representation for parquet-cli compatibility.

#### 4. LIST type support

Apache parquet-cli has limited support for the standard 3-level LIST structure. It may fail to read standard Parquet files, including some samples from the [official parquet-testing repository](https://github.com/apache/parquet-testing/).

Additionally, parquet-cli strictly expects standard 3-level LIST structures and will fail when encountering legacy "repeated primitive" columns.

```
Caused by: org.apache.parquet.io.ParquetDecodingException: The requested schema is not compatible with the file schema.
incompatible types: required group Repeated (LIST) {
  repeated int32 array;
} != repeated int32 Repeated
```

**Workaround:** Use `parquet-tools retype --repeated-to-list` to convert legacy repeated primitive columns to the standard 3-level LIST structure for better compatibility.

## pqrs

[pqrs](https://github.com/manojkarthick/pqrs) is a Rust-based Parquet file reader.

**Known Issues:**
- TBD

## Python parquet-tools

[parquet-tools](https://pypi.org/project/parquet-tools/) is a Python CLI for inspecting Parquet files.

**Known Issues:**

#### 1. Nanosecond Timestamp Precision

Python parquet-tools uses pandas internally and inherits the same nanosecond timestamp precision issue.

```
pyarrow.lib.ArrowInvalid: Value 1000000001 has non-zero nanoseconds
```

**Workaround:** None available within the tool. Use PyArrow directly or pandas with `dtype_backend='pyarrow'` instead.

## DuckDB

[DuckDB](https://duckdb.org/) is an in-process analytical database that supports Parquet files.

**Known Issues:**

#### 1. BSON Type

DuckDB does not support the BSON converted type.

```
IO Error:
Unsupported converted type (20)
```

**Workaround:** Use `parquet-tools retype --bson-to-string` to convert BSON columns to string representation before reading with DuckDB.

## PyArrow

[PyArrow](https://arrow.apache.org/docs/python/) is the Python binding for Apache Arrow, commonly used for Parquet I/O.

**Known Issues:** None. PyArrow reads parquet-tools files without issues.

```python
import pyarrow.parquet as pq

table = pq.read_table("file.parquet")
data = table.to_pylist()

for row in data:
    print(row)
```

## pandas

[pandas](https://pandas.pydata.org/) uses PyArrow or fastparquet for Parquet support.

**Known Issues:**

#### 1. Nanosecond Timestamp Precision

The default pandas backend has issues with nanosecond precision in timestamps (e.g., INT96 timestamps).

```
pyarrow.lib.ArrowInvalid: Value 1000000001 has non-zero nanoseconds
```

**Workaround:** Use the new pandas "Arrow" backend by specifying `dtype_backend='pyarrow'`:

```python
import pandas as pd
df = pd.read_parquet('file.parquet', dtype_backend='pyarrow')
```

## Polars

[Polars](https://pola.rs/) is a fast DataFrame library with native Parquet support.

**Known Issues:**
- TBD

## General Notes

- TBD
