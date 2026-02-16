# CDCFLOW - PostgreSQL to MongoDB CDC Pipeline

A production-ready Change Data Capture (CDC) pipeline that synchronizes data from PostgreSQL to MongoDB using Kafka, Debezium, and a custom Single Message Transform (SMT) for dynamic collection routing.

## Overview

This project implements a real-time data replication pipeline with the following architecture:

```
PostgreSQL (Northwind DB) 
    ↓ (Debezium CDC)
Kafka (Single Consolidated Topic)
    ↓ (Custom SMT - DynamicCollectionRouter)
MongoDB Atlas (Multiple Collections)
```

### Key Features

- **Single Topic Consolidation**: All PostgreSQL tables stream to one Kafka topic (`<consolidated-topic-name>`)
- **Dynamic Collection Fanout**: Custom SMT routes messages to appropriate MongoDB collections based on source table
- **Soft Deletes**: DELETE operations preserve data with `__deleted: true` marker
- **Clean Documents**: Metadata fields removed from final MongoDB documents
- **Deterministic IDs**: FullKeyStrategy ensures consistent `_id` values for updates
- **Error Tolerance**: Configured to handle and log errors without stopping the pipeline

## Architecture Details

### Components

1. **Apache Kafka 4.1.0** (Scala 2.13, KRaft mode)
   - Single broker on port 9092
   - No ZooKeeper required

2. **Kafka Connect 7.8.0**
   - Debezium PostgreSQL Connector 2.2.1
   - MongoDB Kafka Connector 2.0.1
   - Custom SMT for routing logic

3. **PostgreSQL 15** (Northwind sample database)
   - WAL level: logical
   - 14 tables including customers, orders, products, employees

4. **MongoDB Atlas**
   - 12 collections synced from PostgreSQL tables
   - Supports both inserts/updates and soft deletes

### Custom SMT (DynamicCollectionRouter)

The custom Single Message Transform handles:
- **Envelope Unwrapping**: Extracts `after` field for CREATE/READ/UPDATE operations
- **Collection Routing**: Routes messages to appropriate MongoDB collections based on source table name
- **Business Key Injection**: Adds `_businessKey` field containing primary key values for CDC matching
- **Delete Handling**: Creates tombstone messages for DELETE operations

**Operation Handling:**
- `INSERT/UPDATE`: Document contains latest data + `_businessKey` field
- `DELETE`: Tombstone message with key for deletion

### Custom Write Model Strategies

The pipeline uses two custom MongoDB write strategies to enable CDC with auto-generated ObjectIds:

#### UpsertByBusinessKeyStrategy
Handles INSERT and UPDATE operations by:
- **Matching on Business Key**: Uses `_businessKey` field (not `_id`) to find existing documents
- **Preserving ObjectId**: Allows MongoDB to auto-generate ObjectId for `_id` field on inserts
- **Safe Updates**: Removes `_id` from update operations to prevent immutable field errors
- **Upsert Logic**: Inserts new documents or updates existing ones based on business key match
- **Composite Key Support**: Handles both single and multi-field primary keys

#### DeleteByBusinessKeyStrategy
Handles DELETE operations by:
- **Primary Key Matching**: Matches documents using source table primary key fields
- **Clean Deletes**: Removes documents from MongoDB when deleted in PostgreSQL
- **Composite Key Support**: Handles multi-field primary keys with AND filters

**Benefits:**
- MongoDB collections have clean ObjectId-based `_id` fields (compatible with existing applications)
- CDC operations match on business keys instead of `_id`, avoiding nested document IDs
- Supports tables with both single and composite primary keys
- Eliminates need to modify `_id` during updates

## Prerequisites

- Docker & Docker Compose
- MongoDB Atlas account (or MongoDB instance)
- 8GB RAM recommended
- Ports available: 9092 (Kafka), 8083 (Kafka Connect), 5434 (PostgreSQL)

## Setup Instructions

### 1. Clone and Configure

```bash
git clone <repository-url>
cd cdcflow
```

### 2. Set Up Environment Variables

Copy the example environment file:

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```env
# MongoDB Atlas Configuration
MONGODB_USER=your_username
MONGODB_PASSWORD=your_password
MONGODB_CLUSTER=cluster0.xxxxx.mongodb.net
MONGODB_DATABASE=testdb

# PostgreSQL Configuration
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=northwind
POSTGRES_PORT=5434
```

### 3. Build Custom Components

```bash
cd custom-smt
mvn clean package
cd ..
```

This creates two JARs in `custom-smt/target/`:
- `kafka-connect-dynamic-router-1.0.0-smt.jar` - Custom SMT
- `kafka-connect-dynamic-router-1.0.0-strategy.jar` - Custom write strategies

### 4. Start Infrastructure

```bash
docker-compose up -d
```

Wait 60-90 seconds for all services to start. The custom Docker image automatically:
- Installs Debezium PostgreSQL and MongoDB connectors
- Bakes in the custom strategy and SMT JARs
- No manual JAR copying needed!

Check Kafka Connect is ready:
```bash
curl http://localhost:8083/
```

### 5. Deploy Connectors

Deploy both connectors:

```bash
# Deploy PostgreSQL Source Connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/postgres-source.json

# Deploy MongoDB Sink Connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/mongodb-sink-with-strategy.json
```

### 6. Verify Setup

Check connector status:
```bash
curl -s http://localhost:8083/connectors/postgres-source-connector/status | jq
curl -s http://localhost:8083/connectors/mongodb-sink-connector/status | jq
```

Both should show `"state": "RUNNING"`.

## Testing the Pipeline

### Insert Test Data

```bash
docker exec -it postgres-migrator-sample psql -U postgres -d northwind -c \
  "INSERT INTO products (product_id, product_name, supplier_id, category_id, unit_price) 
   VALUES (999, 'Test Product', 1, 1, 25.99);"
```

### Verify in MongoDB

```bash
mongosh "mongodb+srv://<user>:<password>@<cluster>/<database>?retryWrites=true&w=majority" \
  --eval "db.EMPLOYEES.findOne({id: 1})"
```

Expected result:
```javascript
{
  _id: ObjectId("507f1f77bcf86cd799439011"),  // Auto-generated ObjectId
  _businessKey: { id: 1 },                    // Business key for CDC matching
  id: 1,                                      // Original primary key
  name: "John Smith",
  department: "Engineering",
  salary: 85000,
  hired_date: 18276
}
```

### Test Update

```bash
docker exec postgres-source psql -U postgres -d northwind -c \
  "UPDATE \"EMPLOYEES\" SET salary = 90000 WHERE id = 1;"
```

Verify in MongoDB (same ObjectId preserved):
```javascript
{
  _id: ObjectId("507f1f77bcf86cd799439011"),  // Same ObjectId as before
  _businessKey: { id: 1 },
  id: 1,
  name: "John Smith",
  department: "Engineering",
  salary: 90000,                              // Updated value
  hired_date: 18276
}
```

### Test Delete

```bash
docker exec postgres-source psql -U postgres -d northwind -c \
  "DELETE FROM \"EMPLOYEES\" WHERE id = 1;"
```

Verify deletion (document removed from MongoDB):
```bash
mongosh ... --eval "db.EMPLOYEES.findOne({id: 1})"
# Returns: null
```

## Configuration Details

### PostgreSQL Source Connector

- **Connector**: Debezium PostgreSQL 2.2.1
- **Plugin**: pgoutput (built-in logical replication)
- **Snapshot Mode**: initial (captures existing data on first run)
- **Transform**: RegexRouter consolidates all tables → single topic

Key settings:
```json
{
  "database.hostname": "host.docker.internal",
  "database.port": "5434",
  "database.dbname": "northwind",
  "slot.name": "debezium_northwind_slot",
  "topic.prefix": "northwind-source",
  "transforms.routeRows.replacement": "<consolidated-topic-name>"
}
```

### MongoDB Sink Connector

- **Connector**: MongoDB Kafka Connector 2.0.1
- **Write Strategy**: ReplaceOneDefaultStrategy
- **ID Strategy**: FullKeyStrategy (creates nested `_id` from key)
- **Namespace Mapper**: FieldPathNamespaceMapper (routes via `__collection` field)

Key settings:
```json
{
  "transforms.route.type": "com.releaseone.kafka.connect.transforms.DynamicCollectionRouter",
  "namespace.mapper.value.collection.field": "__collection",
  "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.FullKeyStrategy",
  "value.projection.list": "__collection",
  "errors.tolerance": "all"
}
```

## Limitations & Trade-offs

### Soft Deletes Only

Real MongoDB deletes are **not possible** with this architecture due to a circular dependency:

- **Real deletes** require: `delete.on.null.values=true` + null value (tombstone)
- **Collection routing** requires: `value.__collection` field (non-null value)
- **Result**: Cannot have both in single-topic architecture

**Solution**: Soft deletes with `__deleted: true` marker

### Nested _id Structure

The `_id` field uses FullKeyStrategy, resulting in:
```javascript
_id: { product_id: 77 }        // For products
_id: { customer_id: "ALFKI" }  // For customers
```

This is necessary for:
- Deterministic matching across CREATE/UPDATE/DELETE operations
- Support for multiple tables with different primary keys
- Consistent updates to the same document

## Troubleshooting

### Connector Status Issues

Check connector logs:
```bash
curl -s http://localhost:8083/connectors/mongodb-sink-connector/status | \
  jq '.tasks[0].trace'
```

View Kafka Connect logs:
```bash
docker logs kafka-connect | tail -50
```

### Replication Slot Issues

If PostgreSQL replication slot becomes stale:
```bash
docker exec -it postgres-migrator-sample psql -U postgres -d northwind -c \
  "SELECT pg_drop_replication_slot('debezium_northwind_slot');"
```

Then restart the source connector.

### Reset Kafka Connect Offsets

To reprocess messages from the beginning:
```bash
docker exec kafka-connect kafka-consumer-groups --bootstrap-server kafka:29092 \
  --group connect-mongodb-sink-connector --reset-offsets --to-earliest --all-topics --execute
```

### Rebuild and Redeploy SMT

If you modify the custom SMT:
```bash
cd custom-smt
mvn clean package
docker cp target/kafka-connect-dynamic-router-1.0.0.jar kafka-connect:/usr/share/java/kafka/
docker restart kafka-connect
sleep 15
# Redeploy connectors
```

## Monitoring

### Check Topic Messages

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic <consolidated-topic-name> \
  --from-beginning --max-messages 5
```

### List Kafka Topics

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### MongoDB Document Counts

```bash
mongosh ... --eval "
  use testdb;
  db.getCollectionNames().forEach(c => {
    print(c + ': ' + db[c].countDocuments({}));
  });
"
```

## Project Structure

```
vanillaflow/
├── connectors/
│   ├── mongodb-sink-with-strategy.json  # MongoDB sink with custom strategies
│   └── postgres-source.json             # Debezium PostgreSQL source
├── custom-smt/
│   ├── src/main/java/com/releaseone/kafka/connect/
│   │   ├── strategies/
│   │   │   ├── UpsertByBusinessKeyStrategy.java   # Custom upsert strategy
│   │   │   └── DeleteByBusinessKeyStrategy.java   # Custom delete strategy
│   │   └── transforms/
│   │       └── DynamicCollectionRouter.java       # Custom SMT
│   └── pom.xml                          # Maven build configuration
├── init.sql/
│   └── init.sql                         # Northwind database schema
├── Dockerfile.kafka-connect             # Custom Kafka Connect image with strategies
├── docker-compose.yml                   # Infrastructure orchestration
├── .env                                 # Environment variables (git-ignored)
├── .env.example                         # Template for environment setup
└── README.md                            # This file
```

### Key Files

**Custom Strategies:**
- `UpsertByBusinessKeyStrategy.java` - Handles INSERT/UPDATE operations with ObjectId-based _id fields
- `DeleteByBusinessKeyStrategy.java` - Handles DELETE operations matching on business keys
- Both strategies are compiled into `kafka-connect-dynamic-router-1.0.0-strategy.jar`

**Custom SMT:**
- `DynamicCollectionRouter.java` - Routes messages and adds business key fields
- Compiled into `kafka-connect-dynamic-router-1.0.0-smt.jar`

**Docker Configuration:**
- `Dockerfile.kafka-connect` - Bakes custom JARs into Kafka Connect image
- Strategy JAR placed in MongoDB connector's lib directory for classloader access
- SMT JAR placed in Kafka's classpath for transformation access
├── connectors/
│   ├── postgres-source.json      # Debezium source connector config
│   └── mongodb-sink.json          # MongoDB sink connector config
├── custom-smt/
│   ├── src/main/java/com/releaseone/kafka/connect/transforms/
│   │   └── DynamicCollectionRouter.java
│   └── pom.xml
├── docker-compose.yml             # Infrastructure definition
├── deploy-connectors.sh           # Helper script to deploy connectors
├── .env.example                   # Template for environment variables
└── README.md
```

## License

MIT

## Support

For issues or questions:
1. Check connector status: `curl http://localhost:8083/connectors/<connector-name>/status`
2. Review logs: `docker logs kafka-connect`
3. Verify environment variables are set correctly in `.env`
