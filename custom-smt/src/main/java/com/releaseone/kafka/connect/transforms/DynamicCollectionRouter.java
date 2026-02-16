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
 * 4. Adds _businessKey field with the primary key value for MongoDB matching
 * 5. Handles all CRUD operations including real deletes
 * 
 * This SMT runs in the SINK connector and changes the topic metadata
 * so the MongoDB connector can route to the correct collection using
 * the topic name, which works even for tombstones (null values).
 * 
 * The _businessKey field allows MongoDB's ReplaceOneBusinessKeyStrategy
 * to match documents for updates/deletes regardless of the actual
 * primary key column name in different source tables.
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

        // Get the table name - this will be added to message for collection routing
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
                // Primary key fields are already included in 'after'
                newValue = new HashMap<>(after);
                
                // Add _businessKey field for MongoDB matching
                // This field is NOT from Oracle - it's added for CDC matching purposes
                if (record.key() != null && record.key() instanceof Map) {
                    Map<String, Object> keyMap = (Map<String, Object>) record.key();
                    
                    // Create composite key structure for matching
                    Map<String, Object> businessKey = new HashMap<>(keyMap);
                    newValue.put("_businessKey", businessKey);
                }
                
                // Return record with table name as topic for collection routing
                return record.newRecord(
                    tableName,  // Use table name as topic
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),  // Keep original key
                    null, // schema-less
                    newValue,
                    record.timestamp()
                );

            case "d": // Delete
                // For deletes, use table name as topic for routing
                // MongoDB connector will perform real delete when it sees:
                // - delete.on.null.values=true
                // - null value (tombstone)
                Map<String, Object> newKey = new HashMap<>();
                if (record.key() != null && record.key() instanceof Map) {
                    newKey.putAll((Map<String, Object>) record.key());
                }
                
                return record.newRecord(
                    tableName,  // Use table name as topic
                    record.kafkaPartition(),
                    record.keySchema(),
                    newKey,
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
