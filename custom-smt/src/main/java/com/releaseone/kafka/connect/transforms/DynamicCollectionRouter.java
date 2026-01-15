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
 * 3. Changes the topic name to the table name for collection routing
 * 4. Handles all CRUD operations including real deletes
 * 
 * This SMT runs in the SINK connector and changes the topic metadata
 * so the MongoDB connector can route to the correct collection using
 * the topic name, which works even for tombstones (null values).
 */
public class DynamicCollectionRouter<R extends ConnectRecord<R>> implements Transformation<R> {

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration needed
    }

    @Override
    public R apply(R record) {
        // Skip if no value (tombstone) - but still need to extract topic from key
        if (record.value() == null) {
            // This is a tombstone (second message in delete sequence)
            // Pass through as-is - the previous delete event already set the topic
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

        // Get the table name - this becomes our new topic name
        String tableName = (String) source.get("table");
        if (tableName == null) {
            return record;
        }

        // Create the new value based on operation type
        Map<String, Object> newValue;

        switch (op != null ? op : "") {
            case "c": // Create
            case "r": // Read (snapshot)
            case "u": // Update
                // Extract the 'after' field
                Map<String, Object> after = (Map<String, Object>) value.get("after");
                if (after == null) {
                    return record;
                }
                
                // Create new value with just the after fields (no metadata)
                newValue = new HashMap<>(after);
                
                // Return new record with CHANGED TOPIC NAME
                return record.newRecord(
                    tableName.toLowerCase(),  // TOPIC NAME = table name for routing!
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    null, // schema-less
                    newValue,
                    record.timestamp()
                );

            case "d": // Delete
                // For deletes, return null value (tombstone) with CHANGED TOPIC NAME
                // MongoDB connector will perform real delete when it sees:
                // - delete.on.null.values=true
                // - null value (tombstone)
                // - topic name for collection routing
                return record.newRecord(
                    tableName.toLowerCase(),  // TOPIC NAME = table name for routing!
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    null, // schema
                    null, // null value = tombstone for real delete
                    record.timestamp()
                );

            default:
                // Unknown operation, pass through unchanged
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
