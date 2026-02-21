package com.releaseone.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
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
 * Supports both schema-less (Map) and schema-enabled (Struct) records.
 */
public class DynamicCollectionRouter<R extends ConnectRecord<R>> implements Transformation<R> {



    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration needed
    }

    @Override
    public R apply(R record) {
        // Skip tombstones - pass them through unchanged
        if (record.value() == null) {
            return record;
        }
        
        // Handle schema-less JSON (Map)
        if (record.valueSchema() == null) {
            return applySchemaless(record);
        }

        return applyWithSchema(record);
    }

    @SuppressWarnings("unchecked")
    private R applySchemaless(R record) {
        Map<String, Object> value = (Map<String, Object>) record.value();
        if (value == null) {
            return record;
        }

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
                newValue = new HashMap<>(after);
                
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
                // For deletes, emit a tombstone while routing by table name.
                // Keep the key so the sink can identify which document to delete.
                return record.newRecord(
                    tableName,
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    null,
                    null,
                    record.timestamp()
                );

            default:
                // Unknown operation, pass through unchanged
                return record;
        }
    }

    private R applyWithSchema(R record) {
        if (!(record.value() instanceof Struct)) {
            return record;
        }

        Struct envelope = (Struct) record.value();
        String op = envelope.getString("op");

        Struct source = envelope.getStruct("source");
        if (source == null) {
            return record;
        }

        String tableName = source.getString("table");
        if (tableName == null) {
            return record;
        }

        switch (op != null ? op : "") {
            case "c":
            case "r":
            case "u":
                Struct after = envelope.getStruct("after");
                if (after == null) {
                    return record;
                }
                // Keep the table-routed topic, preserve key and typed "after" schema/value.
                return record.newRecord(
                    tableName,
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    after.schema(),
                    after,
                    record.timestamp()
                );

            case "d":
                // Emit tombstone for delete processing while preserving key and routed topic.
                return record.newRecord(
                    tableName,
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    null,
                    null,
                    record.timestamp()
                );

            default:
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
