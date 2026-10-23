# Compatibility

This document tracks known compatibility issues between parquet-tools and other Parquet utilities.

## Table of Contents

- [Apache parquet-cli / PySpark](#apache-parquet-cli--pyspark)
- [arrow-rs/parquet](#arrow-rsparquet)
- [DuckDB](#duckdb)
- [hyparquet](#hyparquet)
- [pandas](#pandas)
- [Polars](#polars)
- [PyArrow](#pyarrow)
- [Python parquet-tools](#python-parquet-tools)
- [Non-standard LIST and MAP Layouts](#non-standard-list-and-map-layouts)

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

#### 5. LIST Whose Repeated Group Holds a Repeated Field

Spark returns wrong values, with no error, for a LIST whose repeated group has a single field that is itself repeated. Values move between rows, and the vectorized and non-vectorized readers give different wrong results. See [Non-standard LIST and MAP Layouts](#non-standard-list-and-map-layouts).

**Workaround:** None. `parquet-tools transcode` writes the same structure, `<name> (LIST) > repeated group list > repeated element`, which Spark misreads the same way.

## arrow-rs/parquet

[arrow-rs](https://github.com/apache/arrow-rs) is the official Rust implementation of Apache Arrow and Parquet.

As of version 57.2.0, `arrow-rs` supports all primitive, logical, and converted types that `parquet-tools` supports.

**Known Issues:**

#### 1. Outdated Downstream Tools (pqrs)

Tools built on `arrow-rs`, such as [pqrs](https://github.com/manojkarthick/pqrs), may use older versions of the library. For example, `pqrs` is built on a 2-year old version of `arrow` (51.0.0), which misses many new Parquet features.

#### 2. LIST Whose Repeated Group Holds a Repeated Field

arrow-rs 60.0.0 reads a LIST whose repeated group has a single repeated field as a list of lists, not the list of structs the spec describes. No values are lost. See [Non-standard LIST and MAP Layouts](#non-standard-list-and-map-layouts).

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

#### 2. Non-finite Floats in JSON Output

DuckDB reads `NaN` and the infinities out of a Parquet file correctly, but its JSON export writes them as the bare literals `NaN`, `Infinity`, and `-Infinity`, which JSON does not allow. `parquet-tools import` rejects such a file.

```
$ duckdb -c "copy (select dbl from read_parquet('non-finite.parquet')) to 'duck.jsonl' (format json)"
$ parquet-tools import -f jsonl -m schema.json -s duck.jsonl out.parquet
parquet-tools: error: invalid JSON string: {"dbl":NaN}
```

**Workaround:** Cast the column to `VARCHAR` in the DuckDB query (`select dbl::varchar`). That writes `"nan"`, `"inf"`, and `"-inf"`, which `import` accepts case-insensitively.

The other direction needs help too: parquet-tools writes those values as the quoted strings `"NaN"`, `"Infinity"`, and `"-Infinity"`, so `read_json` infers a `JSON` column rather than a `DOUBLE` one. Name the types, and DuckDB parses the strings back into non-finite doubles.

```sql
select * from read_json('cat.jsonl', columns={dbl: 'DOUBLE'});
```

#### 3. NaN Drops Column Statistics

DuckDB omits min/max altogether from any float column holding a `NaN`, so `parquet-tools meta` and `inspect` report no bounds for it. Columns holding only infinities keep their bounds. Files written by parquet-tools keep the bounds either way, leaving out only the `NaN` itself, so the same data shows `-Infinity`/`Infinity` there. Both are valid: the Parquet specification forbids writing `NaN` as a min or max but says nothing about the remaining values.

#### 4. LIST and MAP Annotated Only by Logical Type

DuckDB reads a LIST or MAP group that has a logical type but no converted type as a plain struct of its wrapper groups. It also reads a LIST whose repeated group holds a repeated field as a list of lists, not a list of structs. See [Non-standard LIST and MAP Layouts](#non-standard-list-and-map-layouts).

## hyparquet

[hyparquet](https://github.com/hyparam/hyparquet) is a dependency-free TypeScript Parquet reader for the browser and Node. Verified against version 1.28.1.

**Known Issues:**

#### 1. Compression Codecs Are Not Bundled

hyparquet itself decodes only uncompressed and Snappy data (`parquet unsupported compression codec: GZIP`).

**Workaround:** Pass the companion [hyparquet-compressors](https://github.com/hyparam/hyparquet-compressors) package as the reader's `compressors` option.

#### 2. LZ4 and LZ4_RAW Decode Failures

Some LZ4 and LZ4_RAW files fail even with hyparquet-compressors (`lz4 offset out of range`), while others of the same codec read cleanly.

**Workaround:** Use `parquet-tools transcode -z SNAPPY` to rewrite with a codec that round-trips reliably.

#### 3. BSON and INTERVAL Types

hyparquet supports neither converted type (`parquet bson not supported`, `parquet interval not supported`).

**Workaround:** Use `parquet-tools retype --bson-to-string` for BSON. There is no conversion for INTERVAL, so that column has to be dropped.

#### 4. Modular Encryption

hyparquet cannot read encrypted files at all: an encrypted footer no longer ends with the `PAR1` magic it expects (`parquet file invalid (footer != PAR1)`), and encrypted columns fail once page headers cannot be parsed.

**Workaround:** Use `parquet-tools transcode --key-file` to write a plaintext copy. A footer key alone is not enough when columns carry their own keys.

#### 5. Non-finite Floats Serialize as null

hyparquet decodes `NaN` and the infinities into the right JavaScript numbers, but `JSON.stringify` has no way to spell them and writes `null`, which erases the difference between `NaN`, `±Infinity`, and a genuine null.

**Workaround:** Convert them in a replacer, which yields the same spellings parquet-tools uses:

```js
JSON.stringify(rows, (k, v) => typeof v === 'number' && !Number.isFinite(v) ? String(v) : v)
```

#### 6. LIST and MAP Annotated Only by Logical Type

hyparquet reads a LIST or MAP group that has a logical type but no converted type as a plain object of its wrapper groups. It reads a LIST whose repeated group holds a repeated field as a list of lists, and an empty inner list there comes back as `null`. See [Non-standard LIST and MAP Layouts](#non-standard-list-and-map-layouts).

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

#### 2. Non-finite Floats Are Lost in JSON and Confused with NULL

`DataFrame.to_json` writes `NaN` and both infinities as `null`, so a JSON dump cannot be told apart from one of a column full of nulls. The default backend also stores a `NaN` and a NULL as the same value: `isna()` is `True` for both. The Arrow backend keeps them apart (`nan` vs `<NA>`), but its `to_json` still collapses everything to `null`.

```python
df = pd.read_parquet('non-finite.parquet', columns=['dbl'], dtype_backend='pyarrow')
df.dbl.isna().tolist()      # [False, ...] - the NaN is a value, not a null
df.to_json(orient='records')  # [{"dbl":null}, ...]
```

**Workaround:** Use `parquet-tools cat` when the distinction matters; it spells the values `"NaN"`, `"Infinity"`, and `"-Infinity"` and leaves nulls as `null`.

## Polars

[Polars](https://pola.rs/) is a fast DataFrame library with native Parquet support.

**Known Issues:**
- TBD

## PyArrow

[PyArrow](https://arrow.apache.org/docs/python/) is the Python binding for Apache Arrow, commonly used for Parquet I/O.

**Known Issues:** None. PyArrow reads parquet-tools files without issues, non-finite floats included.

Note that `to_pylist()` hands back plain Python floats, so `json.dumps` writes `NaN`, `Infinity`, and `-Infinity` as bare literals, which is not valid JSON and which `parquet-tools import` rejects. Pass `allow_nan=False` to make `json.dumps` raise instead, or map the values to strings to match the parquet-tools spelling.

```python
import pyarrow.parquet as pq

table = pq.read_table("file.parquet")
data = table.to_pylist()

for row in data:
    print(row)
```

## Python parquet-tools

[parquet-tools](https://pypi.org/project/parquet-tools/) is a Python CLI for inspecting Parquet files.

**Known Issues:**

#### 1. Nanosecond Timestamp Precision

Python parquet-tools uses pandas internally and inherits the same nanosecond timestamp precision issue.

```
pyarrow.lib.ArrowInvalid: Value 1000000001 has non-zero nanoseconds
```

**Workaround:** None available within the tool. Use PyArrow directly or pandas with `dtype_backend='pyarrow'` instead.

## Non-standard LIST and MAP Layouts

The Parquet specification allows several LIST and MAP layouts besides the 3-level `<name> (LIST) > repeated group list > element` that most writers produce, and readers disagree on them. `testdata/list-map-layout.parquet`, generated by `testdata/gen/list-map-layout`, holds one column per layout:

| Column | Layout |
| --- | --- |
| `athena` | LIST whose repeated group and element are named `bag` and `array_element`, as Athena writes |
| `athena_logical` | Same as `athena`, annotated only by the LIST logical type |
| `logical_list` | Standard 3-level LIST annotated only by the LIST logical type |
| `logical_map` | Standard MAP annotated only by the MAP logical type |
| `legacy` | `legacy (LIST) > repeated group list > repeated int32 element`. The repeated group's only field is repeated, so per the spec the group `list` is the element itself |
| `entries_map` | MAP whose repeated group is named `entries` instead of `key_value`, annotated by both converted and logical type |
| `entries_logical` | Same as `entries_map`, annotated only by the MAP logical type |

`converted_type` is optional, so a group annotated only by its logical type is still a LIST or MAP, and the spec does not fix the names of a MAP's repeated group. For `legacy`, the spec's backward-compatibility rules make each element a struct holding an `element` list. The first row should read as `[{"element":[1,2]},{"element":[3]}]` and the second as `[{"element":[]}]`.

| Reader | `athena` | `athena_logical` / `logical_list` / `logical_map` | `entries_map` / `entries_logical` | `legacy` |
| --- | --- | --- | --- | --- |
| parquet-tools v1.56.3 (parquet-go v3.9.2) | Struct of wrappers | Struct of wrappers | Struct / struct | `[[1,2],[3]]`, `[[]]` |
| parquet-tools, next release (parquet-go v3.9.4) | List | List / map | Struct / struct | List of structs, as the spec reads it |
| PyArrow 26.0.0 | List | List / map | Map / map | List of structs, as the spec reads it |
| Polars 2.0.0 | List | List / map | Map / map | List of structs, as the spec reads it |
| arrow-rs 60.0.0 | List | List / map | Map / map | List of lists: `[[1,2],[3]]`, `[[]]` |
| DuckDB 1.5.6 | List | Struct of wrappers | Map / struct | List of lists: `[[1,2],[3]]`, `[[]]` |
| hyparquet 1.28.1 | List | Struct of wrappers | Map / struct | List of lists: `[[1,2],[3]]`, `[null]` |
| Spark 4.2.0 | List | List / map | Map / map | **Wrong values**: `[1,2]`, `[3]` (vectorized reader) or `[2,3]`, `[null]` (non-vectorized) |

`parquet-tools cat` renders `legacy` the way PyArrow and Polars do, in both value modes. parquet-go v3.9.3 rendered it as a struct, `{"list":[{"element":[1,2]},{"element":[3]}]}`, until [hangxie/parquet-go#541](https://github.com/hangxie/parquet-go/pull/541) fixed it in v3.9.4.

parquet-go reads a MAP as a map only when its repeated group and fields are named `key_value`, `key`, and `value`, so parquet-tools shows the `entries` columns as structs holding an `entries` list.

`parquet-tools schema` describes each column the way parquet-go reads it, because `transcode` and `merge` write the rows they read through that schema. That is why `schema` shows `legacy` as a list of `int32` lists in every version, the shape arrow-rs, DuckDB, and hyparquet use. It also means the files those commands write differ from the source:

- LIST and MAP groups carry only the converted type, not the logical type.
- LIST wrappers are renamed `list`, so `athena` comes out as `athena (LIST) > repeated group list > array_element`.
- `legacy` keeps its exact structure and LIST converted type: `legacy (LIST) > repeated group list > repeated int32 element`.
- The `entries` columns keep their structure but lose the MAP annotation, so other readers then see a struct instead of a map. The values are preserved.

## General Notes

- TBD
