package org.connector;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Map;

import static org.apache.kafka.connect.data.SchemaBuilder.struct;

public class SchemaConverter {

    public Schema convert(Map<String, Object> record) {
        SchemaBuilder schemaBuilder = struct();
        record.forEach((key, value) -> {
            if (value instanceof String) {
                schemaBuilder.field(key, Schema.STRING_SCHEMA);
            } else if (value instanceof Integer) {
                schemaBuilder.field(key, Schema.INT64_SCHEMA);
            }
        });

        return schemaBuilder.build();
    }
}
