#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate a Parquet file with an UNKNOWN (null) type column.

In Parquet format, UNKNOWN type is used for columns that contain only null values.
This is represented as pa.null() type in PyArrow. The UNKNOWN physical type was
added in Parquet format version 2.4.0.
"""

import pyarrow as pa
import pyarrow.parquet as pq


def main():
    # Create a schema with an UNKNOWN (null) type column
    # In Parquet, UNKNOWN type maps to pa.null() in PyArrow
    schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("unknown_col", pa.null()),
            pa.field("name", pa.string()),
        ]
    )

    # Create data - the unknown_col will contain only nulls
    data = {
        "id": [1, 2, 3, 4, 5],
        "unknown_col": [None, None, None, None, None],
        "name": ["alice", "bob", "charlie", "david", "eve"],
    }

    # Build the table with explicit schema to ensure null type is used
    arrays = [
        pa.array(data["id"], type=pa.int32()),
        pa.nulls(len(data["id"])),  # Creates a null-type array
        pa.array(data["name"], type=pa.string()),
    ]
    table = pa.Table.from_arrays(arrays, schema=schema)

    # Write to parquet file with version 2.6 to ensure UNKNOWN type is supported
    # store_schema=False prevents Arrow schema from being embedded, which could
    # cause type coercion on read
    pq.write_table(
        table,
        "unknown-type.parquet",
        compression="snappy",
        version="2.6",
        store_schema=False,
    )

    print("Generated unknown-type.parquet")


if __name__ == "__main__":
    main()
