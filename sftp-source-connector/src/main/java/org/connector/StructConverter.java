package org.connector;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.Map;

public class StructConverter {

    public Struct convert(Map<String, Object> record, Schema schema) {
        Struct struct = new Struct(schema);
        record.forEach((key, value) -> {
            if (value instanceof String) {
                struct.put(key, value);
            } else if (value instanceof Integer) {
                struct.put(key, value);
            }
        });
        return struct;
    }
}
