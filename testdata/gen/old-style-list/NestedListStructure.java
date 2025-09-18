package org.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NestedListStructure {

  public static void main(String[] args) {
    Schema schema = new Schema.Parser().parse("""
      {
        "type": "record",
        "name": "my_record",
        "fields": [
          {
            "name": "first",
            "type": {
              "type": "record",
              "name": "first_record",
              "fields": [
                {
                  "name": "second",
                  "type": {
                    "type": "record",
                    "name": "second_record",
                    "fields": [
                      {
                        "name": "a",
                        "type": {
                          "type": "array",
                          "items": {
                            "type": "array",
                            "items": "int"
                          }
                        }
                      }
                    ]
                  }
                }
              ]
            }
          }
        ]
      }
    """);

    Path file = new Path("./old-style-list.parquet");
    Configuration conf = new Configuration();
    conf.set("parquet.avro.write-old-list-structure", "true");  // this is the default value
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(file)
      .withSchema(schema)
      .withConf(conf)
      .build()) {

      // Write 3 rows with different data
      for (int rowNum = 1; rowNum <= 3; rowNum++) {
        GenericRecord record = new GenericData.Record(schema);

        // Create the nested structure
        Schema firstSchema = schema.getField("first").schema();
        GenericRecord firstRecord = new GenericData.Record(firstSchema);

        Schema secondSchema = firstSchema.getField("second").schema();
        GenericRecord secondRecord = new GenericData.Record(secondSchema);

        // Write different data for each row
        java.util.List<java.util.List<Integer>> rowData;
        if (rowNum == 1) {
          rowData = Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4));
        } else if (rowNum == 2) {
          rowData = Arrays.asList(Arrays.asList(5, 6), Arrays.asList(7, 8));
        } else {
          rowData = Arrays.asList(Arrays.asList(9, 10), Arrays.asList(11, 12));
        }

        secondRecord.put("a", rowData.stream()
          .map(list -> {
            Schema innerListType = secondSchema.getField("a").schema().getElementType();
            GenericData.Array<Integer> innerList = new GenericData.Array<>(list.size(), innerListType);
            innerList.addAll(list);
            return innerList;
          }).collect(Collectors.toList()));

        firstRecord.put("second", secondRecord);
        record.put("first", firstRecord);

        writer.write(record);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
