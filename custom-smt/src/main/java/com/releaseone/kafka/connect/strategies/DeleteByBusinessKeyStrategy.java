package com.releaseone.kafka.connect.strategies;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.writemodel.strategy.DeleteOneDefaultStrategy;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.Map;

/**
 * Custom delete strategy that matches documents using _businessKey field
 * instead of _id for delete operations in CDC scenarios.
 */
public class DeleteByBusinessKeyStrategy extends DeleteOneDefaultStrategy {

    @Override
    public WriteModel<BsonDocument> createWriteModel(SinkDocument document) {
        // Extract business key from the key document (Debezium sends PK in key)
        BsonDocument keyDoc = document.getKeyDoc()
            .orElseThrow(() -> new DataException("Missing key document for delete operation"));

        if (keyDoc.isEmpty()) {
            throw new DataException("Empty key document - cannot determine which document to delete");
        }

        // Build filter using all fields from the key document
        Bson filter = buildFilterFromKey(keyDoc);

        return new DeleteOneModel<>(filter);
    }

    /**
     * Build MongoDB filter from key fields
     * Handles both single and composite keys
     */
    private Bson buildFilterFromKey(BsonDocument keyDoc) {
        if (keyDoc.size() == 1) {
            // Single field - create simple equality filter on that field name
            Map.Entry<String, BsonValue> entry = keyDoc.entrySet().iterator().next();
            return Filters.eq(entry.getKey(), entry.getValue());
        } else {
            // Composite key - create AND filter with all fields
            Bson[] filters = keyDoc.entrySet().stream()
                .map(entry -> Filters.eq(entry.getKey(), entry.getValue()))
                .toArray(Bson[]::new);
            return Filters.and(filters);
        }
    }
}
