package com.releaseone.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
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
 * Includes optional type normalization using Debezium source column type hints.
 *
 * Connector config controls:
 * - transforms.route.normalization.enabled=true|false
 * - transforms.route.type.mapping.mode=oracle|postgres
 */
public class DynamicCollectionRouter<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final String NORMALIZATION_ENABLED_CONFIG = "normalization.enabled";
    private static final String TYPE_MAPPING_MODE_CONFIG = "type.mapping.mode";
    private static final String MODE_ORACLE = "oracle";
    private static final String MODE_POSTGRES = "postgres";

    // Oracle mode mappings (source type -> target type).
    private static final Map<String, String> ORACLE_TYPE_MAPPING = new LinkedHashMap<>();
    static {
        ORACLE_TYPE_MAPPING.put("VARCHAR2", "string");   // Postgres: VARCHAR, TEXT
        ORACLE_TYPE_MAPPING.put("CHAR", "string");       // Postgres: CHAR, CHARACTER
        ORACLE_TYPE_MAPPING.put("NCHAR", "string");      // Postgres equivalent: CHAR, CHARACTER
        ORACLE_TYPE_MAPPING.put("NVARCHAR2", "string");  // Postgres: VARCHAR
        ORACLE_TYPE_MAPPING.put("INTEGER", "int");       // Postgres: INTEGER, INT4
        ORACLE_TYPE_MAPPING.put("FLOAT", "double");      // Postgres: DOUBLE PRECISION
        ORACLE_TYPE_MAPPING.put("BINARY_FLOAT", "double");  // Postgres: REAL
        ORACLE_TYPE_MAPPING.put("BINARY_DOUBLE", "double"); // Postgres: DOUBLE PRECISION
        ORACLE_TYPE_MAPPING.put("DATE", "date");         // Postgres: DATE
        ORACLE_TYPE_MAPPING.put("TIMESTAMP", "date");    // Postgres: TIMESTAMP, TIMESTAMPTZ
        ORACLE_TYPE_MAPPING.put("CLOB", "string");       // Postgres: TEXT
        ORACLE_TYPE_MAPPING.put("NCLOB", "string");      // Postgres: TEXT
        ORACLE_TYPE_MAPPING.put("BLOB", "binData");      // Postgres: BYTEA
        ORACLE_TYPE_MAPPING.put("RAW", "binData");       // Postgres: BYTEA
        ORACLE_TYPE_MAPPING.put("LONG RAW", "binData");  // Postgres: BYTEA
    }

    // Postgres mode mappings (source type -> target type).
    private static final Map<String, String> POSTGRES_TYPE_MAPPING = new LinkedHashMap<>();
    static {
        POSTGRES_TYPE_MAPPING.put("VARCHAR", "string");      // Oracle: VARCHAR2
        POSTGRES_TYPE_MAPPING.put("CHAR", "string");         // Oracle: CHAR/NCHAR
        POSTGRES_TYPE_MAPPING.put("CHARACTER", "string");    // Oracle: CHAR/NCHAR
        POSTGRES_TYPE_MAPPING.put("TEXT", "string");         // Oracle: CLOB/NCLOB

        POSTGRES_TYPE_MAPPING.put("INTEGER", "int");         // Oracle: INTEGER
        POSTGRES_TYPE_MAPPING.put("INT4", "int");            // Oracle: INTEGER
        POSTGRES_TYPE_MAPPING.put("SMALLINT", "int");        // Oracle nearest: NUMBER(5)
        POSTGRES_TYPE_MAPPING.put("INT2", "int");            // Oracle nearest: NUMBER(5)

        POSTGRES_TYPE_MAPPING.put("DOUBLE PRECISION", "double"); // Oracle: BINARY_DOUBLE/FLOAT
        POSTGRES_TYPE_MAPPING.put("FLOAT8", "double");           // Oracle: BINARY_DOUBLE/FLOAT
        POSTGRES_TYPE_MAPPING.put("REAL", "double");             // Oracle: BINARY_FLOAT
        POSTGRES_TYPE_MAPPING.put("FLOAT4", "double");           // Oracle: BINARY_FLOAT
        POSTGRES_TYPE_MAPPING.put("NUMERIC", "double");          // Oracle: NUMBER
        POSTGRES_TYPE_MAPPING.put("DECIMAL", "double");          // Oracle: NUMBER

        POSTGRES_TYPE_MAPPING.put("DATE", "date");               // Oracle: DATE
        POSTGRES_TYPE_MAPPING.put("TIMESTAMP", "date");          // Oracle: TIMESTAMP
        POSTGRES_TYPE_MAPPING.put("TIMESTAMPTZ", "date");        // Oracle: TIMESTAMP WITH TIME ZONE
        POSTGRES_TYPE_MAPPING.put("TIMESTAMP WITH TIME ZONE", "date"); // Oracle: TIMESTAMP WITH TIME ZONE

        POSTGRES_TYPE_MAPPING.put("BYTEA", "binData");           // Oracle: BLOB/RAW/LONG RAW
    }

    private boolean normalizationEnabled = true;
    private String mappingMode = MODE_ORACLE;
    private Map<String, String> activeTypeMapping = ORACLE_TYPE_MAPPING;

    @Override
    public void configure(Map<String, ?> configs) {
        Object raw = configs.get(NORMALIZATION_ENABLED_CONFIG);
        if (raw == null) {
            normalizationEnabled = true;
            return;
        }
        if (raw instanceof Boolean) {
            normalizationEnabled = (Boolean) raw;
        } else {
            normalizationEnabled = Boolean.parseBoolean(String.valueOf(raw));
        }

        Object rawMode = configs.get(TYPE_MAPPING_MODE_CONFIG);
        String configuredMode = rawMode == null ? MODE_ORACLE : String.valueOf(rawMode).trim().toLowerCase(Locale.ROOT);
        if (!MODE_ORACLE.equals(configuredMode) && !MODE_POSTGRES.equals(configuredMode)) {
            configuredMode = MODE_POSTGRES;
        }
        mappingMode = configuredMode;
        activeTypeMapping = MODE_ORACLE.equals(mappingMode) ? ORACLE_TYPE_MAPPING : POSTGRES_TYPE_MAPPING;
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
                Struct normalizedAfter = normalizationEnabled ? normalizeStruct(after) : after;
                // Keep the table-routed topic, preserve key and typed "after" schema/value.
                return record.newRecord(
                    tableName,
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    normalizedAfter.schema(),
                    normalizedAfter,
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

    private Struct normalizeStruct(Struct after) {
        Struct normalized = new Struct(after.schema());
        for (Field field : after.schema().fields()) {
            Object rawValue = after.get(field);
            String mappedTargetType = mappedTargetType(field.schema());
            Object normalizedValue = normalizeValue(rawValue, mappedTargetType);
            normalized.put(field, normalizedValue);
        }
        return normalized;
    }

    private String mappedTargetType(Schema schema) {
        if (schema == null || schema.parameters() == null) {
            return null;
        }

        String sourceType = null;
        for (Map.Entry<String, String> entry : schema.parameters().entrySet()) {
            String key = entry.getKey();
            if (key != null && key.toLowerCase().contains("column.type")) {
                sourceType = entry.getValue();
                break;
            }
        }
        if (sourceType == null || sourceType.isEmpty()) {
            return null;
        }

        String normalized = normalizeSourceType(sourceType);
        return activeTypeMapping.get(normalized);
    }

    private String normalizeSourceType(String sourceType) {
        String type = sourceType.trim().toUpperCase();
        int parenIndex = type.indexOf('(');
        if (parenIndex >= 0) {
            type = type.substring(0, parenIndex).trim();
        }
        return type;
    }

    private Object normalizeValue(Object value, String targetType) {
        if (value == null || targetType == null) {
            return value;
        }

        switch (targetType) {
            case "string":
                return String.valueOf(value);
            case "int":
                return toInteger(value);
            case "double":
                return toDouble(value);
            case "date":
                return toDate(value);
            case "binData":
                return toBinary(value);
            default:
                return value;
        }
    }

    private Object toInteger(Object value) {
        if (value instanceof Integer) {
            return value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                return value;
            }
        }
        return value;
    }

    private Object toDouble(Object value) {
        if (value instanceof Double) {
            return value;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                return value;
            }
        }
        return value;
    }

    private Object toDate(Object value) {
        if (value instanceof Date) {
            return value;
        }
        if (value instanceof Number) {
            long numeric = ((Number) value).longValue();
            long abs = Math.abs(numeric);
            if (abs < 5_000_000L) {
                Instant instant = LocalDate.ofEpochDay(numeric).atStartOfDay(ZoneOffset.UTC).toInstant();
                return Date.from(instant);
            }
            if (abs < 100_000_000_000L) {
                return Date.from(Instant.ofEpochSecond(numeric));
            }
            return new Date(numeric);
        }
        if (value instanceof String) {
            String raw = (String) value;
            try {
                return Date.from(Instant.parse(raw));
            } catch (DateTimeParseException e) {
                try {
                    return Date.from(LocalDate.parse(raw).atStartOfDay(ZoneOffset.UTC).toInstant());
                } catch (DateTimeParseException ignored) {
                    return value;
                }
            }
        }
        return value;
    }

    private Object toBinary(Object value) {
        if (value instanceof byte[]) {
            return value;
        }
        if (value instanceof ByteBuffer) {
            ByteBuffer buffer = ((ByteBuffer) value).asReadOnlyBuffer();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }
        if (value instanceof String) {
            try {
                return Base64.getDecoder().decode((String) value);
            } catch (IllegalArgumentException e) {
                return value;
            }
        }
        return value;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
            .define(
                NORMALIZATION_ENABLED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.MEDIUM,
                "Enable/disable datatype normalization based on source column type hints."
            )
            .define(
                TYPE_MAPPING_MODE_CONFIG,
                ConfigDef.Type.STRING,
                MODE_ORACLE,
                ConfigDef.ValidString.in(MODE_POSTGRES, MODE_ORACLE),
                ConfigDef.Importance.MEDIUM,
                "Datatype mapping mode: 'postgres' or 'oracle'."
            );
    }

    @Override
    public void close() {
        // No resources to close
    }
}
