package com.releaseone.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.transforms.Transformation;

import java.nio.ByteBuffer;
import java.math.BigDecimal;
import java.math.BigInteger;
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
import java.util.concurrent.ConcurrentHashMap;

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
 * Performance note:
 * - Normalization plans are cached per schema for both value and key.
 * - If a schema needs no conversion, records are passed through without cloning.
 * - Dynamic decimal scale cases still preserve correctness by using per-scale schema cache.
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
    // Note: for Oracle numeric families, runtime precision/scale logic enforces
    // the effective behavior used by this SMT:
    // - integer-like NUMBER with precision <= 18 -> long (INT64)
    // - numeric with precision > 18 or scale > 0 -> decimal
    private static final Map<String, String> ORACLE_TYPE_MAPPING = new LinkedHashMap<>();
    static {
        ORACLE_TYPE_MAPPING.put("VARCHAR2", "string");   // Postgres: VARCHAR, TEXT
        ORACLE_TYPE_MAPPING.put("CHAR", "string");       // Postgres: CHAR, CHARACTER
        ORACLE_TYPE_MAPPING.put("NCHAR", "string");      // Postgres equivalent: CHAR, CHARACTER
        ORACLE_TYPE_MAPPING.put("NVARCHAR2", "string");  // Postgres: VARCHAR
        ORACLE_TYPE_MAPPING.put("INTEGER", "int");
        ORACLE_TYPE_MAPPING.put("NUMBER", "decimal");    // Runtime override: <=18 integer-like -> long, otherwise decimal
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
    // Numeric mappings here are baseline defaults.
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
    // Value schema -> precomputed conversion plan used on the hot path.
    private final Map<Schema, ValueNormalizationPlan> valuePlanCache = new ConcurrentHashMap<>();
    // Key schema -> precomputed key normalization plan for update/delete matching.
    private final Map<Schema, KeyNormalizationPlan> keyPlanCache = new ConcurrentHashMap<>();

    @Override
    public void configure(Map<String, ?> configs) {
        Object raw = configs.get(NORMALIZATION_ENABLED_CONFIG);
        if (raw == null) {
            normalizationEnabled = true;
        } else if (raw instanceof Boolean) {
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
        // Drop cached plans whenever config changes to avoid stale mapping behavior.
        valuePlanCache.clear();
        keyPlanCache.clear();
    }

    @Override
    public R apply(R record) {
        // Skip tombstones - pass them through unchanged
        if (record.value() == null) {
            return record;
        }
        
        // Handle schema-less JSON (Map)
        if (record.valueSchema() == null) {
            return applySchemaless(record); //REMOVE? Place on dead letter queue instead
        }

        return applyWithSchema(record);
    }

    @SuppressWarnings("unchecked")
    private R applySchemaless(R record) {
        Map<String, Object> value = (Map<String, Object>) record.value();
        if (value == null) {
            return record;
        }
        Object normalizedKey = normalizationEnabled ? normalizeSchemalessKey(record.key()) : record.key();

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
                    normalizedKey,
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
                    normalizedKey,
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
        SchemaAndValue normalizedKey = normalizationEnabled
            ? normalizeStructuredKey(record.keySchema(), record.key())
            : new SchemaAndValue(record.keySchema(), record.key());

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
                    normalizedKey.schema(),
                    normalizedKey.value(),
                    normalizedAfter.schema(),
                    normalizedAfter,
                    record.timestamp()
                );

            case "d":
                // Emit tombstone for delete processing while preserving key and routed topic.
                return record.newRecord(
                    tableName,
                    record.kafkaPartition(),
                    normalizedKey.schema(),
                    normalizedKey.value(),
                    null,
                    null,
                    record.timestamp()
                );

            default:
                return record;
        }
    }

    private Struct normalizeStruct(Struct after) {
        // Build conversion plan once per schema, then reuse for all records with same schema.
        ValueNormalizationPlan plan = valuePlanCache.computeIfAbsent(after.schema(), this::buildValuePlan);
        // Fast path: no field in this schema requires normalization.
        if (!plan.requiresNormalization) {
            return after;
        }

        Schema normalizedSchema = plan.fixedNormalizedSchema != null
            ? plan.fixedNormalizedSchema
            : buildDynamicValueSchema(after, plan);
        Struct normalized = new Struct(normalizedSchema);

        for (int i = 0; i < plan.fields.length; i++) {
            Field field = plan.fields[i];
            Object rawValue = after.get(field);
            String targetType = plan.targetTypes[i];
            Object normalizedValue = targetType == null
                ? rawValue
                : normalizeValue(rawValue, targetType, field.schema());
            normalized.put(normalizedSchema.field(field.name()), normalizedValue);
        }
        return normalized;
    }

    private ValueNormalizationPlan buildValuePlan(Schema schema) {
        // Precompute target type and normalized field schema decisions once per value schema.
        Field[] fields = schema.fields().toArray(new Field[0]);
        String[] targetTypes = new String[fields.length];
        Schema[] fixedFieldSchemas = new Schema[fields.length];
        boolean[] dynamicSchemaField = new boolean[fields.length];

        boolean requiresNormalization = false;
        boolean hasDynamicFieldSchema = false;
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            String targetType = mappedTargetType(field.schema());
            targetTypes[i] = targetType;

            if (targetType == null) {
                fixedFieldSchemas[i] = field.schema();
                continue;
            }

            requiresNormalization = true;
            if (isDynamicDecimalScaleField(field.schema(), targetType)) {
                dynamicSchemaField[i] = true;
                hasDynamicFieldSchema = true;
                fixedFieldSchemas[i] = field.schema();
            } else {
                fixedFieldSchemas[i] = buildFieldSchema(field.schema(), targetType, null);
            }
        }

        Schema fixedNormalizedSchema = null;
        if (requiresNormalization && !hasDynamicFieldSchema) {
            fixedNormalizedSchema = buildStructSchema(schema, fields, fixedFieldSchemas);
        }
        return new ValueNormalizationPlan(fields, targetTypes, fixedFieldSchemas, dynamicSchemaField, requiresNormalization, fixedNormalizedSchema);
    }

    private Schema buildDynamicValueSchema(Struct after, ValueNormalizationPlan plan) {
        // Only used for fields whose normalized schema depends on per-record content (e.g., variable scale).
        Schema[] fieldSchemas = new Schema[plan.fields.length];
        for (int i = 0; i < plan.fields.length; i++) {
            if (plan.dynamicSchemaField[i]) {
                Object raw = after.get(plan.fields[i]);
                fieldSchemas[i] = buildFieldSchema(plan.fields[i].schema(), plan.targetTypes[i], raw);
            } else {
                fieldSchemas[i] = plan.fixedFieldSchemas[i];
            }
        }
        return buildStructSchema(after.schema(), plan.fields, fieldSchemas);
    }

    private Schema buildStructSchema(Schema originalSchema, Field[] fields, Schema[] fieldSchemas) {
        // Shared helper that preserves schema metadata while swapping field schemas.
        SchemaBuilder builder = SchemaBuilder.struct();
        if (originalSchema.name() != null) {
            builder.name(originalSchema.name());
        }
        if (originalSchema.version() != null) {
            builder.version(originalSchema.version());
        }
        if (originalSchema.doc() != null) {
            builder.doc(originalSchema.doc());
        }
        for (int i = 0; i < fields.length; i++) {
            builder.field(fields[i].name(), fieldSchemas[i]);
        }
        return builder.build();
    }

    private boolean isDynamicDecimalScaleField(Schema schema, String targetType) {
        return schema != null
            && "decimal".equals(targetType)
            && "io.debezium.data.VariableScaleDecimal".equals(schema.name());
    }

    private Schema buildFieldSchema(Schema originalFieldSchema, String targetType, Object rawValue) {
        if (originalFieldSchema == null || targetType == null) {
            return originalFieldSchema;
        }

        SchemaBuilder builder;
        switch (targetType) {
            case "string":
                builder = SchemaBuilder.string();
                break;
            case "int":
                builder = SchemaBuilder.int32();
                break;
            case "long":
                builder = SchemaBuilder.int64();
                break;
            case "double":
                builder = SchemaBuilder.float64();
                break;
            case "date":
                builder = Timestamp.builder();
                break;
            case "binData":
                builder = SchemaBuilder.bytes();
                break;
            case "decimal":
                if ("io.debezium.data.VariableScaleDecimal".equals(originalFieldSchema.name())) {
                    Integer scale = extractVariableScale(rawValue);
                    builder = Decimal.builder(scale == null ? 0 : scale);
                    break;
                }
            default:
                return originalFieldSchema;
        }

        if (originalFieldSchema.isOptional()) {
            builder.optional();
        }
        if (originalFieldSchema.doc() != null) {
            builder.doc(originalFieldSchema.doc());
        }
        return builder.build();
    }

    private String mappedTargetType(Schema schema) {
        if (schema == null) {
            return null;
        }
        Map<String, String> parameters = schema.parameters();
        if (parameters == null) {
            parameters = new HashMap<>();
        }

        // Connect Decimal logical type path (e.g. decimal.handling.mode=precise).
        if ("org.apache.kafka.connect.data.Decimal".equals(schema.name())) {
            return mapNumericByPrecisionScale(parameters);
        }
        // Debezium Oracle NUMBER without explicit precision/scale can arrive as VariableScaleDecimal.
        if ("io.debezium.data.VariableScaleDecimal".equals(schema.name())) {
            return "decimal";
        }
        // Debezium Postgres INTERVAL logical type path.
        if ("io.debezium.time.MicroDuration".equals(schema.name())) {
            return "string";
        }

        String sourceType = null;
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String key = entry.getKey();
            if (key != null && key.toLowerCase().contains("column.type")) {
                sourceType = entry.getValue();
                break;
            }
        }
        if (sourceType == null || sourceType.isEmpty()) {
            // If source metadata is missing, upcast primitive small integers to long.
            // Restrict to non-logical INT8/INT16/INT32 so Timestamp (logical INT64) is untouched.
            if (isPrimitiveIntegerSchema(schema)) {
                return "long";
            }
            return null;
        }

        String normalized = normalizeSourceType(sourceType);

        // Handle timestamp variations (e.g. TIMESTAMP(6), TIMESTAMP WITH TIME ZONE).
        if (normalized.startsWith("TIMESTAMP")) {
            return "date";
        }
        // Handle interval variations (e.g. INTERVAL DAY TO SECOND).
        if (normalized.startsWith("INTERVAL")) {
            return "string";
        }

        // Precision/scale-aware routing for numeric families.
        if ("NUMBER".equals(normalized) || "NUMERIC".equals(normalized) || "DECIMAL".equals(normalized)) {
            return mapNumericByPrecisionScale(parameters);
        }
        // Default fallback for any recognized source type not explicitly mapped.
        return activeTypeMapping.getOrDefault(normalized, "string");
    }

    private String mapNumericByPrecisionScale(Map<String, String> parameters) {
        Integer scale = parseNumericParameter(parameters, "scale");
        if (scale != null && scale > 0) {
            return "decimal";
        }

        Integer precision = parseNumericParameter(parameters, "precision");
        if (precision != null) {
            if (precision <= 18) {
                return "long";
            }
            return "decimal";
        }

        return "decimal";
    }

    private Integer parseNumericParameter(Map<String, String> parameters, String keyHint) {
        String raw = null;
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String key = entry.getKey();
            if (key != null && key.toLowerCase(Locale.ROOT).contains(keyHint)) {
                raw = entry.getValue();
                break;
            }
        }
        if (raw == null) {
            return null;
        }
        if (raw == null || raw.trim().isEmpty()) {
            return null;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String normalizeSourceType(String sourceType) {
        String type = sourceType.trim().toUpperCase();
        int parenIndex = type.indexOf('(');
        if (parenIndex >= 0) {
            type = type.substring(0, parenIndex).trim();
        }
        return type;
    }

    private boolean isPrimitiveIntegerSchema(Schema schema) {
        if (schema == null || schema.type() == null) {
            return false;
        }
        // Skip logical types like org.apache.kafka.connect.data.Timestamp (INT64).
        if (schema.name() != null && !schema.name().isEmpty()) {
            return false;
        }
        switch (schema.type()) {
            case INT8:
            case INT16:
            case INT32:
                return true;
            default:
                return false;
        }
    }

    private Object normalizeValue(Object value, String targetType, Schema sourceSchema) {
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
            case "long":
                return toLong(value);
            case "date":
                return toDate(value);
            case "binData":
                return toBinary(value);
            case "decimal":
                if (sourceSchema != null && "io.debezium.data.VariableScaleDecimal".equals(sourceSchema.name())) {
                    return variableScaleDecimalToBigDecimal(value);
                }
                return value;
            default:
                return value;
        }
    }

    private Integer extractVariableScale(Object value) {
        if (!(value instanceof Struct)) {
            return null;
        }
        Struct decimalStruct = (Struct) value;
        return decimalStruct.getInt32("scale");
    }

    @SuppressWarnings("unchecked")
    private Object normalizeSchemalessKey(Object key) {
        if (!(key instanceof Map)) {
            return key;
        }
        Map<String, Object> keyMap = (Map<String, Object>) key;
        Map<String, Object> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : keyMap.entrySet()) {
            Object raw = entry.getValue();
            if (raw instanceof Map) {
                Map<String, Object> rawMap = (Map<String, Object>) raw;
                if (rawMap.containsKey("scale") && rawMap.containsKey("value")) {
                    normalized.put(entry.getKey(), schemalessVariableScaleToBigDecimal(rawMap));
                    continue;
                }
            }
            normalized.put(entry.getKey(), raw);
        }
        return normalized;
    }

    private Object schemalessVariableScaleToBigDecimal(Map<String, Object> valueMap) {
        Object scaleObj = valueMap.get("scale");
        Object rawObj = valueMap.get("value");
        if (!(scaleObj instanceof Number)) {
            return valueMap;
        }
        byte[] bytes = toBytes(rawObj);
        if (bytes == null) {
            return valueMap;
        }
        int scale = ((Number) scaleObj).intValue();
        return new BigDecimal(new BigInteger(bytes), scale);
    }

    private SchemaAndValue normalizeStructuredKey(Schema keySchema, Object keyValue) {
        if (keySchema == null || !(keyValue instanceof Struct)) {
            return new SchemaAndValue(keySchema, keyValue);
        }

        // Build key plan once per key schema and skip work if key does not contain variable-scale fields.
        KeyNormalizationPlan plan = keyPlanCache.computeIfAbsent(keySchema, this::buildKeyPlan);
        if (!plan.requiresNormalization) {
            return new SchemaAndValue(keySchema, keyValue);
        }

        Struct keyStruct = (Struct) keyValue;
        // Cache normalized key schema by observed decimal-scale signature.
        String scaleSignature = buildScaleSignature(keyStruct, plan.fields, plan.variableScaleField);
        Schema normalizedSchema = plan.schemaByScaleSignature.computeIfAbsent(
            scaleSignature,
            ignored -> buildNormalizedKeySchema(keySchema, plan.fields, plan.variableScaleField, plan.upcastLongField, keyStruct)
        );
        Struct normalizedStruct = new Struct(normalizedSchema);
        for (Field field : plan.fields) {
            Object raw = keyStruct.get(field);
            normalizedStruct.put(field.name(), normalizeStructuredKeyFieldValue(field.schema(), raw, plan.upcastLongField[field.index()]));
        }
        return new SchemaAndValue(normalizedSchema, normalizedStruct);
    }

    private KeyNormalizationPlan buildKeyPlan(Schema keySchema) {
        // Detect if this key schema has fields that require structural key normalization.
        Field[] fields = keySchema.fields().toArray(new Field[0]);
        boolean[] variableScaleField = new boolean[fields.length];
        boolean[] upcastLongField = new boolean[fields.length];
        boolean requiresNormalization = false;
        for (int i = 0; i < fields.length; i++) {
            Schema schema = fields[i].schema();
            boolean isVariableScale = schema != null && "io.debezium.data.VariableScaleDecimal".equals(schema.name());
            variableScaleField[i] = isVariableScale;
            boolean shouldUpcastLong = isPrimitiveIntegerSchema(schema) && "long".equals(mappedTargetType(schema));
            upcastLongField[i] = shouldUpcastLong;
            if (isVariableScale || shouldUpcastLong) {
                requiresNormalization = true;
            }
        }
        return new KeyNormalizationPlan(fields, variableScaleField, upcastLongField, requiresNormalization);
    }

    private String buildScaleSignature(Struct struct, Field[] fields, boolean[] variableScaleField) {
        // Compact cache key representing current per-field variable decimal scales.
        StringBuilder signature = new StringBuilder(fields.length * 3);
        for (int i = 0; i < fields.length; i++) {
            if (!variableScaleField[i]) {
                signature.append('|');
                continue;
            }
            Integer scale = extractVariableScale(struct.get(fields[i]));
            signature.append(scale == null ? "n" : scale).append('|');
        }
        return signature.toString();
    }

    private Schema buildNormalizedKeySchema(
        Schema keySchema,
        Field[] fields,
        boolean[] variableScaleField,
        boolean[] upcastLongField,
        Struct keyStruct
    ) {
        // Build a key schema variant that matches the normalized key value shape.
        SchemaBuilder builder = SchemaBuilder.struct();
        if (keySchema.name() != null) {
            builder.name(keySchema.name());
        }
        if (keySchema.version() != null) {
            builder.version(keySchema.version());
        }
        if (keySchema.doc() != null) {
            builder.doc(keySchema.doc());
        }
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            Schema normalizedFieldSchema = buildNormalizedKeyFieldSchema(
                field.schema(),
                keyStruct.get(field),
                variableScaleField[i],
                upcastLongField[i]
            );
            builder.field(field.name(), normalizedFieldSchema);
        }
        return builder.build();
    }

    private Schema buildNormalizedKeyFieldSchema(
        Schema fieldSchema,
        Object rawValue,
        boolean variableScaleField,
        boolean upcastLongField
    ) {
        if (variableScaleField && fieldSchema != null && "io.debezium.data.VariableScaleDecimal".equals(fieldSchema.name())) {
            Integer scale = extractVariableScale(rawValue);
            SchemaBuilder decimalBuilder = Decimal.builder(scale == null ? 0 : scale);
            if (fieldSchema.isOptional()) {
                decimalBuilder.optional();
            }
            if (fieldSchema.doc() != null) {
                decimalBuilder.doc(fieldSchema.doc());
            }
            return decimalBuilder.build();
        }
        if (upcastLongField && fieldSchema != null) {
            SchemaBuilder longBuilder = SchemaBuilder.int64();
            if (fieldSchema.isOptional()) {
                longBuilder.optional();
            }
            if (fieldSchema.doc() != null) {
                longBuilder.doc(fieldSchema.doc());
            }
            return longBuilder.build();
        }
        return fieldSchema;
    }

    private Object normalizeStructuredKeyFieldValue(Schema fieldSchema, Object rawValue, boolean upcastLongField) {
        if (fieldSchema != null && "io.debezium.data.VariableScaleDecimal".equals(fieldSchema.name())) {
            return variableScaleDecimalToBigDecimal(rawValue);
        }
        if (upcastLongField) {
            return toLong(rawValue);
        }
        return rawValue;
    }

    private Object variableScaleDecimalToBigDecimal(Object value) {
        if (!(value instanceof Struct)) {
            return null;
        }
        Struct decimalStruct = (Struct) value;
        Integer scale = decimalStruct.getInt32("scale");
        Object raw = decimalStruct.get("value");
        if (raw == null || scale == null) {
            return null;
        }

        byte[] bytes = toBytes(raw);
        if (bytes == null) {
            return null;
        }
        return new BigDecimal(new BigInteger(bytes), scale);
    }

    private byte[] toBytes(Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
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
                return null;
            }
        }
        return null;
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

    private Object toLong(Object value) {
        if (value instanceof Long) {
            return value;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
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
        // Clear local caches on task shutdown/rebalance.
        valuePlanCache.clear();
        keyPlanCache.clear();
    }

    private static final class ValueNormalizationPlan {
        // Immutable plan for a value schema to avoid repeated per-record schema introspection.
        private final Field[] fields;
        private final String[] targetTypes;
        private final Schema[] fixedFieldSchemas;
        private final boolean[] dynamicSchemaField;
        private final boolean requiresNormalization;
        private final Schema fixedNormalizedSchema;

        private ValueNormalizationPlan(
            Field[] fields,
            String[] targetTypes,
            Schema[] fixedFieldSchemas,
            boolean[] dynamicSchemaField,
            boolean requiresNormalization,
            Schema fixedNormalizedSchema
        ) {
            this.fields = fields;
            this.targetTypes = targetTypes;
            this.fixedFieldSchemas = fixedFieldSchemas;
            this.dynamicSchemaField = dynamicSchemaField;
            this.requiresNormalization = requiresNormalization;
            this.fixedNormalizedSchema = fixedNormalizedSchema;
        }
    }

    private static final class KeyNormalizationPlan {
        // Immutable key-field plan plus per-scale derived schema cache.
        private final Field[] fields;
        private final boolean[] variableScaleField;
        private final boolean[] upcastLongField;
        private final boolean requiresNormalization;
        private final Map<String, Schema> schemaByScaleSignature = new ConcurrentHashMap<>();

        private KeyNormalizationPlan(
            Field[] fields,
            boolean[] variableScaleField,
            boolean[] upcastLongField,
            boolean requiresNormalization
        ) {
            this.fields = fields;
            this.variableScaleField = variableScaleField;
            this.upcastLongField = upcastLongField;
            this.requiresNormalization = requiresNormalization;
        }
    }
}
