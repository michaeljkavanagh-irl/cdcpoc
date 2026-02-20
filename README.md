# CDCFLOW - PostgreSQL to MongoDB CDC Pipeline

Real-time CDC pipeline from PostgreSQL (Northwind) to MongoDB using Debezium, Kafka, Kafka Connect, and a custom SMT for dynamic collection routing.

## Overview

```text
PostgreSQL (Northwind)
  -> Debezium (source connector)
  -> Kafka topic: northwind.clean.topic
  -> MongoDB sink connector (+ custom SMT)
  -> MongoDB Atlas collections (table-per-collection routing)
```

## Key Features

- Single consolidated Kafka topic for all captured tables (`northwind.clean.topic`)
- Dynamic collection routing by table name using `DynamicCollectionRouter`
- End-to-end CRUD support, including hard deletes in MongoDB
- DLQ configured for sink processing errors (`mongodb-sink-dlq`)
- Secrets sourced from `.env` (no hardcoded connector credentials)

## Components

1. Kafka + Kafka Connect
2. Debezium PostgreSQL connector
3. MongoDB Kafka sink connector
4. Custom Java extension JARs:
- `DynamicCollectionRouter` (SMT)
- `DeleteByBusinessKeyStrategy` (custom delete write model strategy)

## CRUD Flow

- `INSERT/UPDATE`:
  - Source emits Debezium envelope.
  - SMT unwraps `after` and routes topic to table name.
  - Sink writes/upserts document in target MongoDB collection.
- `DELETE`:
  - Source emits delete event and tombstone (`tombstones.on.delete=true`).
  - SMT routes delete by table name and emits `value=null` (tombstone) while preserving key.
  - Sink processes tombstone with `delete.on.null.values=true` and deletes matching document.

## Delete Strategy (Current)

Hard deletes are implemented with tombstones plus business-key matching:

1. Debezium produces a delete (`op: "d"`) for the row key and then a tombstone.
2. `DynamicCollectionRouter` uses `source.table` to route to the target collection topic and preserves the key for delete matching.
3. MongoDB sink receives `value=null` and triggers delete processing (`delete.on.null.values=true`).
4. `DeleteByBusinessKeyStrategy` builds a Mongo filter directly from Kafka key fields and executes `DeleteOne`.

This avoids relying on Mongo `_id` for delete matching and supports both single and composite keys.

## Connector Configuration Notes

### Source (`/Users/michael.kavanagh/mongo/vanillaflow/connectors/postgres-source.json`)

Important settings:

```json
{
  "tombstones.on.delete": "true",
  "transforms.routeRows.replacement": "northwind.clean.topic"
}
```

### Sink (`/Users/michael.kavanagh/mongo/vanillaflow/connectors/mongodb-sink-builtin-strategy.json`)

Recommended delete-related settings:

```json
{
  "transforms": "route",
  "transforms.route.type": "com.releaseone.kafka.connect.transforms.DynamicCollectionRouter",
  "delete.on.null.values": "true",
  "delete.writemodel.strategy": "com.releaseone.kafka.connect.strategies.DeleteByBusinessKeyStrategy"
}
```

If `delete.writemodel.strategy` is set to a MongoDB built-in class instead, deletes will follow that built-in behavior instead of the custom business-key strategy.

## Setup

1. Create `.env` from template and fill credentials:

```bash
cp .env.example .env
```

2. Build custom JARs:

```bash
cd custom-smt
mvn clean package
cd ..
```

3. Start/rebuild infrastructure:

```bash
docker compose up -d --build
```

4. Deploy connectors (POST for first deploy, PUT for updates):

```bash
curl -sS -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/postgres-source.json

curl -sS -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/mongodb-sink-builtin-strategy.json
```

## Smoke Test

Insert:

```bash
docker exec postgres-source psql -U postgres -d northwind -c \
  "INSERT INTO \"EMPLOYEES\" (id,last_name,first_name,title,title_of_courtesy,birth_date,hire_date,address,city,region,postal_code,country,home_phone,extension,notes,reports_to,photo_path,salary) VALUES (888900,'Test','Flow','Engineer','Mr.','1990-01-01','2021-06-01','1 Main St','Dublin',NULL,'D01','Ireland','555-0100','100','cdc smoke',2,'/tmp/test',75000);"
```

Update:

```bash
docker exec postgres-source psql -U postgres -d northwind -c \
  "UPDATE \"EMPLOYEES\" SET salary = 76000 WHERE id = 888900;"
```

Delete:

```bash
docker exec postgres-source psql -U postgres -d northwind -c \
  "DELETE FROM \"EMPLOYEES\" WHERE id = 888900;"
```

Verify in MongoDB:

```bash
mongosh "<your-uri>" --quiet --eval 'db.EMPLOYEES.findOne({id: 888900})'
```

Expected after delete: `null`

## Troubleshooting

- Connector status:

```bash
curl -sS http://localhost:8083/connectors/postgres-source-connector/status | jq
curl -sS http://localhost:8083/connectors/mongodb-sink-connector/status | jq
```

- Sink DLQ inspection:

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic mongodb-sink-dlq \
  --from-beginning
```

- Kafka Connect logs:

```bash
docker logs kafka-connect-1 --since 10m
```

## Project Structure

```text
vanillaflow/
  connectors/
    postgres-source.json
    mongodb-sink-builtin-strategy.json
  custom-smt/
    src/main/java/com/releaseone/kafka/connect/transforms/DynamicCollectionRouter.java
    src/main/java/com/releaseone/kafka/connect/strategies/DeleteByBusinessKeyStrategy.java
  Dockerfile.kafka-connect
  docker-compose.yml
  .env.example
  README.md
```
