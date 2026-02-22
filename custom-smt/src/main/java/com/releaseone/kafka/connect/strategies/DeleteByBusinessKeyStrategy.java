package com.releaseone.kafka.connect.strategies;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.writemodel.strategy.DeleteOneDefaultStrategy;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

/**
 * Delete strategy that matches Mongo documents by business key fields from Kafka key.
 */
public class DeleteByBusinessKeyStrategy extends DeleteOneDefaultStrategy {
    @Override
    public WriteModel<BsonDocument> createWriteModel(SinkDocument document) {
        BsonDocument keyDoc = document.getKeyDoc()
            .orElseThrow(() -> new DataException("Missing key document for delete operation"));
        System.out.println("[DeleteByBusinessKeyStrategy] keyDoc=" + keyDoc.toJson());

        if (keyDoc.isEmpty()) {
            throw new DataException("Empty key document - cannot determine which document to delete");
        }

        Bson filter = buildFilterFromKey(keyDoc);
        System.out.println("[DeleteByBusinessKeyStrategy] deleteFilter=" + filter);
        return new DeleteOneModel<>(filter);
    }

    private Bson buildFilterFromKey(BsonDocument keyDoc) {
        if (keyDoc.size() == 1) {
            Map.Entry<String, BsonValue> entry = keyDoc.entrySet().iterator().next();
            return Filters.eq(entry.getKey(), normalizeKeyValue(entry.getValue()));
        }

        Bson[] filters = keyDoc.entrySet().stream()
            .map(entry -> Filters.eq(entry.getKey(), normalizeKeyValue(entry.getValue())))
            .toArray(Bson[]::new);
        return Filters.and(filters);
    }

    private Object normalizeKeyValue(BsonValue keyValue) {
        if (!keyValue.isDocument()) {
            System.out.println("[DeleteByBusinessKeyStrategy] normalizeKeyValue passthrough scalar=" + keyValue);
            return keyValue;
        }

        BsonDocument doc = keyValue.asDocument();
        if (!doc.containsKey("scale") || !doc.containsKey("value")) {
            System.out.println("[DeleteByBusinessKeyStrategy] normalizeKeyValue passthrough document=" + doc.toJson());
            return keyValue;
        }

        BsonValue scaleValue = doc.get("scale");
        BsonValue unscaledValue = doc.get("value");
        if (!(scaleValue instanceof BsonInt32) || !(unscaledValue instanceof BsonBinary)) {
            System.out.println("[DeleteByBusinessKeyStrategy] normalizeKeyValue non-standard scale/value document=" + doc.toJson());
            return keyValue;
        }

        int scale = scaleValue.asInt32().getValue();
        byte[] bytes = unscaledValue.asBinary().getData();
        BigInteger unscaled = new BigInteger(bytes);
        BigDecimal decimal = new BigDecimal(unscaled, scale);
        System.out.println("[DeleteByBusinessKeyStrategy] normalizeKeyValue VariableScaleDecimal -> Decimal128, scale=" + scale + ", decimal=" + decimal);
        return new Decimal128(decimal);
    }
}
