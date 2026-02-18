package com.releaseone.kafka.connect.strategies;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.writemodel.strategy.WriteModelStrategy;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.Map;

/**
 * Custom WriteModelStrategy that:
 * 1. Allows MongoDB to auto-generate ObjectId for _id
 * 2. Matches documents using _businessKey field for updates
 * 3. Performs upsert operations (insert if not exists, update if exists)
 * 4. Handles deletes by matching on _businessKey
 */
public class UpsertByBusinessKeyStrategy implements WriteModelStrategy {

    private static final UpdateOptions UPDATE_OPTIONS = new UpdateOptions().upsert(true);
    
    public WriteModel<BsonDocument> createWriteModel(SinkDocument document) {
        BsonDocument valueDoc = document.getValueDoc()
            .orElseThrow(() -> new DataException("Missing value document"));

        // Check if this is a delete (tombstone will be handled separately)
        if (valueDoc.isEmpty()) {
            throw new DataException("Empty value document - tombstones should be handled by deleteOneModel");
        }

        // Extract _businessKey for matching
        BsonDocument businessKey = valueDoc.getDocument("_businessKey", null);
        if (businessKey == null || businessKey.isEmpty()) {
            throw new DataException("Missing _businessKey field in document for matching");
        }

        // Build filter using all fields from _businessKey
        Bson filter = buildFilterFromBusinessKey(businessKey);

        // Remove _id from valueDoc if present (MongoDB doesn't allow modifying _id)
        valueDoc.remove("_id");
        
        // Remove _businessKey from the document before upserting (optional - you can keep it)
        // valueDoc.remove("_businessKey");

        // Create update using $set to update fields, $setOnInsert for _id generation on first insert
        BsonDocument updateDoc = new BsonDocument()
            .append("$set", valueDoc)
            .append("$setOnInsert", new BsonDocument()); // Allows ObjectId generation on insert

        return new UpdateOneModel<>(filter, updateDoc, UPDATE_OPTIONS);
    }

    /**
     * Build MongoDB filter from business key fields
     * Matches on nested _businessKey fields instead of top-level fields
     * Handles both single and composite keys
     */
    private Bson buildFilterFromBusinessKey(BsonDocument businessKey) {
        if (businessKey.size() == 1) {
            // Single field - create filter on nested _businessKey field
            Map.Entry<String, BsonValue> entry = businessKey.entrySet().iterator().next();
            return Filters.eq("_businessKey." + entry.getKey(), entry.getValue());
        } else {
            // Composite key - create AND filter with all nested _businessKey fields
            Bson[] filters = businessKey.entrySet().stream()
                .map(entry -> Filters.eq("_businessKey." + entry.getKey(), entry.getValue()))
                .toArray(Bson[]::new);
            return Filters.and(filters);
        }
    }
}
