package com.releaseone.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.HashMap;
import java.util.Map;

/**
 * Custom SMT that:
 * 1. Extracts the table name from Debezium CDC envelope
 * 2. Unwraps the 'after' field to get the actual record data
 * 3. Adds collection name to message headers for routing (works with tombstones)
 * 4. Handles all CRUD operations (insert, update, delete)
 */
public class DynamicCollectionRouter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String COLLECTION_HEADER_NAME = "__collection";

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration needed
    }

    @Override
    public R apply(R record) {
        // Skip if no value (tombstone) - but still process to keep headers
        if (record.value() == null) {
            // Tombstone - headers should already be set from previous delete event
            return record;
        }

        // Handle schema-less JSON (Map)
        if (record.valueSchema() == null) {
            return applySchemaless(record);
        }

        // For now, only handle schema-less JSON
        return record;
    }

    @SuppressWarnings("unchecked")
    private R applySchemaless(R record) {
        Map<String, Object> value = (Map<String, Object>) record.value();

        // Extract the operation type
        String op = (String) value.get("op");
        
        // Extract source metadata
        Map<String, Object> source = (Map<String, Object>) value.get("source");
        if (source == null) {
            return record;
        }

        // Get the table name
        String tableName = (String) source.get("table");
        if (tableName == null) {
            return record;
        }

        // Create the new value based on operation type
        Map<String, Object> newValue;
        R newRecord;

        switch (op != null ? op : "") {
            case "c": // Create
            case "r": // Read (snapshot)
            case "u": // Update
                // Extract the 'after' field
                Map<String, Object> after = (Map<String, Object>) value.get("after");
                if (after == null) {
                    return record;
                }
                
                // Create new value with just the after fields plus collection for routing
                newValue = new HashMap<>(after);
                newValue.put("__collection", tableName);
                
                // Create new record with collection header
                newRecord = record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    null, // schema-less
                    newValue,
                    record.timestamp()
                );
                
                // Add collection name to headers as well (for future use)
                newRecord.headers().addString(COLLECTION_HEADER_NAME, tableName);
                return newRecord;

            case "d": // Delete
                // For deletes, use 'before' data and add __deleted marker (soft delete)
                Map<String, Object> before = (Map<String, Object>) value.get("before");
                if (before == null) {
                    // If no before data, skip
                    return record;
                }
                
                newValue = new HashMap<>(before);
                newValue.put("__collection", tableName);
                newValue.put("__deleted", true);
                
                newRecord = record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    null, // schema-less
                    newValue,
                    record.timestamp()
                );
                
                // Add collection name to headers
                newRecord.headers().addString(COLLECTION_HEADER_NAME, tableName);
                return newRecord;

            default:
                // Unknown operation, pass through
                return record;
        }
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
        // No resources to close
    }
}
