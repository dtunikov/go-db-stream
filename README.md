<div align="center">

# go-db-stream

### Database change data capture to streams and queues like Kafka, SQS, HTTP endpoints, and more

[![License: MIT](https://img.shields.io/badge/License-MIT-purple.svg)](https://opensource.org/licenses/MIT)

</div>

## What is go-db-stream?

go-db-stream makes it easy to stream changes from your database to streaming platforms and queues like Kafka, SQS, Redis, and HTTP endpoints. Change data capture enables applications and services to track and respond to row-level changes in database tables as they occur. 

## Change data capture use cases

- **Triggering a workflow when data changes in Postgres:** Execute custom business logic whenever specific rows are inserted, updated, or deleted in your database.
- **Making events available to downstream services:** Stream changes from your database tables as events that other services can consume.
- **Informing downstream services when rows change:** Notify dependent services about data changes to keep systems in sync.
- **Audit logging:** Track and record all changes made to data in your database for compliance or feature development.
- **Sync a table from one database to another:** Keep tables synchronized across different database instances in real-time.
- **Materializing another table in your database:** Create and maintain derived tables based on changes to source tables.
- **Maintaining a cache:** Keep caches up-to-date by streaming database changes.
- **Refreshing search indexes:** Keep search indexes fresh by streaming updates from your database.

## Supported databases

### Databases
- ✅ Postgres
- ⚙️ MySQL
- ⚙️ SQL Server
- ⚙️ Oracle
- ⚙️ MongoDB
- ⚙️ Cassandra
- ⚙️ MongoDB
- ⚙️ Elasticsearch
- other databases...

## Sinks
| Sink | Support | Description |
|-------------|---------|-------------|
| Kafka | ✅ Real-time streaming | Stream changes to Apache Kafka topics |
| SQS | ⚙️ Real-time streaming | Send messages to Amazon SQS queues |
| Redis | ⚙️ Real-time streaming | `XADD` to Redis Streams |
| HTTP Webhook | ⚙️ Real-time streaming | Send changes to any HTTP endpoint |
| GCP Pub/Sub | ⚙️ Real-time streaming | Publish messages to Google Cloud Pub/Sub topics |
| NATS | ⚙️ Real-time streaming | Stream changes to NATS subjects |
| RabbitMQ | ⚙️ Real-time streaming | Publish messages to RabbitMQ exchanges |
| Azure EventHubs | Coming soon |
| Amazon SNS | Coming soon |
| AWS Kinesis | Coming soon |
| Other sinks | Coming soon |

## Getting started

To get started, you can follow the examples in the `examples` directory. Each example demonstrates how to stream changes from a database to a sink like Kafka, SQS, or HTTP endpoints.

## How it works?

With go-db-stream, you can connect to any database and start streaming changes from selected tables. You can define optional filters and transformations to customize the data flow. Changes can be routed to various sinks such as Kafka, SQS, Redis, or HTTP endpoints.

During sink setup, you can choose to backfill existing data from the source tables into the sink for a complete dataset. (not yet implemented)

Once configured, go-db-stream streams new changes to the sink in real-time. If delivery issues arise, the system automatically retries with exponential backoff to ensure reliability.

For configuration, you can define setups as code using straightforward YAML configuration files.

## Benchmarks

| Setup | Latency | RPS |
|-------|---------|-----|
| Postgres -> Kafka | 500ms | 100 |

Latency shows how long it takes for the message to be streamed from Postgres to Kafka (Kafka message timestamp - Postgres created_at row field).
See the `examples/[example]/benchmark` directory for benchmarking scripts and instructions on how to run them.

## Contribute

Sequin is open source (MIT license). The project is just getting started, so the best way to contribute right now is to open an issue.